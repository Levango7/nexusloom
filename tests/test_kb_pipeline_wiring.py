"""kb-docgen 流水线接线：摄入 → 建库检索 → 知识库接地写作。

这三段能力（ingest / knowledge_base / writer）此前是**没有接进任何流水线**的
库代码：`grep -rn "ingest|knowledge_base" pipelines/` 零命中，writer 的检索
仍是任务内 TF-IDF，KB 的向量检索根本不参与生成。本文件锁住接线后的行为，
并在离线（无 LLM Key、无网络）条件下跑通整条 DAG。
"""
import os
import shutil
import sys
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

PROJECT = Path(__file__).parent.parent

QUOTA_DOC = """# Kafka 配额设计

## 生产者配额

producer_byte_rate 限制单个客户端每秒可写入的字节数，超限后请求被节流
而非直接拒绝，避免突发流量打爆 broker。

## 集群级配额

cluster_quota 是所有租户共享的上限，配额服务每 15 秒重新计算一次。
"""

PARTITION_DOC = """# Kafka 分区策略

## 分区数选择

分区数决定并行消费上限，过多分区会拉长 leader 选举与端到端延迟。

## 副本放置

跨机架分配副本可以避免单机架故障导致分区不可用。
"""

TOPIC = "Kafka 生产者配额与限流策略"


@pytest.fixture
def corpus(tmp_path):
    c = tmp_path / "corpus"
    c.mkdir()
    (c / "kafka-quota.md").write_text(QUOTA_DOC, encoding="utf-8")
    (c / "kafka-partition.md").write_text(PARTITION_DOC, encoding="utf-8")
    input_file = tmp_path / "input.md"
    input_file.write_text(f"# 主题\n{TOPIC}\n\n# 资料\n{c / 'kafka-quota.md'}\n"
                          f"{c / 'kafka-partition.md'}\n", encoding="utf-8")
    return {"dir": c, "input": input_file, "tmp": tmp_path}


def _payload(**kw) -> dict:
    """构造与 dag_executor._build_node_payload 同形的消息载荷。"""
    base = {"task_id": "t1", "input_file": "", "config": {}, "node": "",
            "queries": [], "dependencies_results": {}, "articles": [],
            "results": [], "content": "", "spec": None}
    base.update(kw)
    return base


def _make_agent(cls, config: dict, name: str):
    from loom_core.base_agent import AgentMeta
    return cls(name, AgentMeta(name=name, version="1.0"), config, None, None)


def _msg(payload: dict):
    from loom_core.base_agent import Message
    return Message(topic="kb.input", payload=payload, from_agent="test")


# ─── 1. 流水线本身可解析、拓扑合法 ────────────────────────

class TestPipelineDefinition:
    def test_kb_docgen_parses_with_expected_levels(self):
        from loom_core.naming import agent_of
        from loom_core.scheduler import Scheduler
        plan = Scheduler().parse_file(str(PROJECT / "packs" / "docgen" / "pipelines" / "kb-docgen.yaml"))
        assert plan.pipeline_name == "kb-docgen"
        # 8 = ingest + knowledge_base + writer + 质量尾内联的五个节点
        # （尾里多出 fact_checker__quality_tail，被 when 条件跳过而非删除）
        assert plan.node_count == 8, plan.node_count
        # 层级按 Agent 还原：片段身份带别名是抽取的预期结果，钉字面名等于每次
        # 改调用方命名都要来动这条结构断言。
        assert [agent_of(n[0].agent_name) for n in plan.levels] == [
            "ingest", "knowledge_base", "writer",
            "quality_gate", "checker", "fact_checker", "layout", "safe_writer"]

    def test_ingest_and_kb_declare_their_own_schema(self):
        """config 类型漂移必须在解析期报错，而不是运行时静默。

        声明由各 Agent 模块持有，Scheduler 用 AST 读取（core 不再集中名单）。
        """
        from loom_core.scheduler import Scheduler
        sched = Scheduler()
        assert "files_from_input" in sched._schema_for_agent("ingest")
        assert "embedder" in sched._schema_for_agent("knowledge_base")

    def test_bad_type_in_config_rejected(self, tmp_path, corpus):
        from loom_core.scheduler import Scheduler
        src = (PROJECT / "packs" / "docgen" / "pipelines" / "kb-docgen.yaml").read_text(encoding="utf-8")
        drifted = src.replace("top_k: 6", 'top_k: "很多"')
        target = tmp_path / "kb-drift.yaml"
        target.write_text(drifted, encoding="utf-8")
        with pytest.raises(TypeError, match="top_k"):
            Scheduler().parse_file(str(target), verify_lock=False)


# ─── 2. 摄入层：资料清单从输入文件解析 ────────────────────

class TestIngestCollectsMaterials:
    def test_paths_from_input_are_resolved_not_the_topic(self, corpus):
        from agents.ingest_agent import IngestAgent
        files = IngestAgent._collect_files(
            _payload(input_file=str(corpus["input"])))
        assert len(files) == 2, f"应恰好解析出 2 份语料，实际: {files}"
        assert all(Path(f).is_file() for f in files)
        assert not any(TOPIC in f for f in files), "主题行被误当成语料文件"

    def test_explicit_config_files_win(self, corpus):
        from agents.ingest_agent import IngestAgent
        chosen = [str(corpus["dir"] / "kafka-quota.md")]
        files = IngestAgent._collect_files(_payload(
            input_file=str(corpus["input"]), config={"files": chosen}))
        assert files == chosen

    def test_missing_input_yields_no_files_and_reports_skip(self, tmp_path):
        from agents.ingest_agent import IngestAgent
        agent = _make_agent(IngestAgent, {"output_dir": str(tmp_path / "out"),
                                          "ocr_enabled": False}, "ingest")
        res = agent.handle(_msg(_payload(input_file=str(tmp_path / "nope.md"),
                                         task_id="t-empty")))
        # 没有语料是"无事可做"，不是失败；但也必须如实标出来，不能记成交付
        assert res["status"] == "skipped"
        assert "未指定待摄入文件" in res["message"]

    def test_bad_input_dir_fails_loudly(self, tmp_path):
        from agents.ingest_agent import IngestAgent
        with pytest.raises(FileNotFoundError, match="input_dir"):
            IngestAgent._collect_files(_payload(
                config={"input_dir": str(tmp_path / "does-not-exist")}))


# ─── 3. 知识库：索引上游产出 + 多查询词检索 ───────────────

class TestKnowledgeBaseNode:
    @pytest.fixture
    def kb_agent(self, corpus):
        from agents.knowledge_base_agent import KnowledgeBaseAgent
        return _make_agent(KnowledgeBaseAgent, {
            "db_path": str(corpus["tmp"] / "kb.db"),
            "embedder": "hash",  # 离线确定性：不拉语义模型
            "top_k": 4,
        }, "knowledge_base")

    def _ingest_dep(self, kb_agent, corpus) -> dict:
        from agents.ingest_agent import IngestAgent
        ing = _make_agent(IngestAgent, {
            "output_dir": str(corpus["tmp"] / "ingested"),
            "ocr_enabled": False,
        }, "ingest")
        res = ing.handle(_msg(_payload(input_file=str(corpus["input"]),
                                       task_id="t1")))
        assert res["status"] == "ok", res
        return res

    def test_index_and_search_over_ingest_output(self, kb_agent, corpus):
        dep = self._ingest_dep(kb_agent, corpus)
        res = kb_agent.handle(_msg(_payload(
            task_id="t1",
            queries=[TOPIC, str(corpus["dir"] / "kafka-quota.md")],
            config={"action": "index_and_search"},
            dependencies_results={"ingest": dep},
        )))
        assert res["status"] == "ok", res
        assert res["indexed"] == 2
        assert res["hits"] > 0
        # 资料清单行被剔除，不作为查询词
        assert res["queries"] == [TOPIC]
        # 命中必须能溯源到用户自己的文件，而不是中间产物
        sources = {Path(str(h.get("source", ""))).name for h in res["results"]}
        assert "kafka-quota.md" in sources, f"命中来源不可溯源: {sources}"

    def test_inferred_search_uses_queries_not_only_single_query(self, kb_agent, corpus):
        """DAG 节点载荷带的是 `queries`，没有单条 `query`。

        实测缺陷：handle 推断出 action=search 后一律调 `_do_search(payload)`，
        那里只读 `query` → 回 `{"status":"error","message":"未指定查询词"}`。
        旧引擎把 error 当成功吞掉，改判业务失败后才把这条断链暴露出来。
        """
        res = kb_agent.handle(_msg(_payload(task_id="t-queries",
                                           queries=[TOPIC])))
        assert res.get("message") != "未指定查询词", (
            f"带 queries 的检索请求被判成无词: {res}")
        assert res["status"] == "ok", res

    def test_search_hits_relevant_chunk_first(self, kb_agent, corpus):
        dep = self._ingest_dep(kb_agent, corpus)
        res = kb_agent.handle(_msg(_payload(
            task_id="t2", queries=["分区数选择 副本放置"],
            config={"action": "index_and_search"},
            dependencies_results={"ingest": dep},
        )))
        assert res["status"] == "ok", res
        top = res["results"][0]
        assert "分区" in top["content"], f"首位命中与查询无关: {top}"

    def test_no_index_and_no_hit_is_an_error_not_empty_success(self, kb_agent):
        res = kb_agent.handle(_msg(_payload(
            task_id="t3", queries=["完全不存在的主题xyz"],
            config={"action": "index_and_search"},
        )))
        assert res["status"] == "error"
        assert res["indexed"] == 0

    def test_material_line_detection(self, kb_agent, tmp_path):
        assert kb_agent._is_material_line("corpus/a.md") is True
        real = tmp_path / "x.pdf"
        real.write_bytes(b"%PDF-1.4")
        assert kb_agent._is_material_line(str(real)) is True
        assert kb_agent._is_material_line(TOPIC) is False
        # 非法路径字符不得抛错（Windows 对保留字符敏感）
        assert kb_agent._is_material_line('a<b>:c|"?') is False


# ─── 4. Writer：KB 命中进入生成上下文 ────────────────────

class TestWriterGrounding:
    HITS = [
        {"chunk_id": "c1", "doc_id": "d1", "ordinal": 0,
         "heading_path": "Kafka 配额设计 > 生产者配额",
         "content": "producer_byte_rate 限制单个客户端每秒可写入的字节数，"
                    "超限后请求被节流而非直接拒绝，避免突发流量打爆 broker。",
         "source": "corpus/kafka-quota.md", "title": "Kafka 配额设计",
         "score": 0.81},
        {"chunk_id": "c2", "doc_id": "d2", "ordinal": 1,
         "heading_path": "Kafka 分区策略 > 分区数选择",
         "content": "分区数决定并行消费上限，过多分区会拉长 leader 选举与端到端延迟。",
         "source": "corpus/kafka-partition.md", "title": "Kafka 分区策略",
         "score": 0.42},
    ]

    def _writer(self, tmp_path):
        from packs.docgen.agents.writer import WriterAgent
        return _make_agent(WriterAgent, {"prompt_profile": "generic-tech",
                                         "polish_cache_ttl": 0}, "writer")

    def test_kb_hits_extracted_from_dependency_results(self):
        from packs.docgen.agents.writer import WriterAgent
        payload = _payload(dependencies_results={"knowledge_base":
                                                 {"results": self.HITS}})
        hits = WriterAgent._kb_hits(payload)
        assert [h["chunk_id"] for h in hits] == ["c1", "c2"]

    def test_empty_content_chunks_are_dropped(self):
        from packs.docgen.agents.writer import WriterAgent
        payload = _payload(dependencies_results={"knowledge_base": {"results": [
            {"chunk_id": "c9", "content": "   ", "source": "x.md"},
            self.HITS[0],
        ]}})
        assert [h["chunk_id"] for h in WriterAgent._kb_hits(payload)] == ["c1"]

    def test_offline_build_is_extractive_and_traceable(self, tmp_path):
        """无 LLM Key 时也必须交付真实内容（抽取式），且带可溯源参考清单。"""
        w = self._writer(tmp_path)
        w._llm_api_key = ""
        with patch("loom_core.llm_router.get_router", return_value=None):
            res = w._build_from_kb(self.HITS, TOPIC, "测试文档", "t1")
        assert res["status"] == "ok"
        assert res["stats"]["llm_used"] is False
        assert res["stats"]["kb_hits"] == 2
        content = res["content"]
        assert "producer_byte_rate" in content
        assert "分区数决定并行消费上限" in content
        assert "## 参考资料（本地知识库）" in content
        assert "corpus/kafka-quota.md" in content

    def test_llm_context_carries_kb_provenance(self, tmp_path):
        """断言 writer 交给 LLM 的素材里带 kb:// 来源与 chunk 标识。

        这条是"KB 真的参与生成"的核心证据：接线前 writer 只吃任务内 TF-IDF。
        """
        w = self._writer(tmp_path)
        w._llm_api_key = "fake-key"
        captured = {}

        def fake_restructure(content, articles, query, title,
                             stream_callback=None, task_id=""):
            captured["articles"] = articles
            return "# 重构文档\n\nproducer_byte_rate 限流\n\n## 参考资料\n\n- 略\n"

        w._restructure_document = fake_restructure  # type: ignore[assignment]
        res = w._build_from_kb(self.HITS, TOPIC, "测试文档", "t1")
        arts = captured["articles"]
        assert len(arts) == 2
        assert arts[0]["url"] == "kb://corpus/kafka-quota.md#c1"
        assert arts[0]["relevance"] == pytest.approx(0.81)
        assert res["stats"]["llm_used"] is True

    def test_handle_routes_to_kb_when_no_articles(self, tmp_path):
        w = self._writer(tmp_path)
        w._llm_api_key = ""
        payload = _payload(dependencies_results={"knowledge_base":
                                                 {"results": self.HITS}},
                           queries=[TOPIC], query=TOPIC,
                           title="测试文档")
        with patch("loom_core.llm_router.get_router", return_value=None):
            res = w.handle(_msg(payload))
        assert res is not None
        assert res["status"] == "ok"
        assert "producer_byte_rate" in res["content"]
        assert "未采集到可整合的搜索结果" not in res["content"]

    def test_articles_still_win_and_warn(self, tmp_path):
        """网络文章优先，但不能静默丢掉 KB —— 必须留警告。"""
        w = self._writer(tmp_path)
        called = {}
        w._build_from_articles = lambda *a, **k: called.setdefault(  # type: ignore[assignment]
            "path", "articles") and {"status": "ok"}
        w.log_warning = lambda m: called.setdefault("warn", m)  # type: ignore[assignment]
        w.handle(_msg(_payload(
            articles=[{"url": "https://e.com", "local_path": "x", "title": "t"}],
            dependencies_results={"knowledge_base": {"results": self.HITS}})))
        assert called.get("path") == "articles"
        assert "KB 接地未生效" in called.get("warn", "")


# ─── 5. 整条 DAG 离线跑通（真实 agent，无 LLM/网络）───────

class TestKbPipelineOfflineE2E:
    def test_full_kb_docgen_run_offline(self, corpus, monkeypatch):
        monkeypatch.chdir(corpus["tmp"])
        # 用 hash 嵌入器改写一份临时 YAML：CI/本机都不许下载语义模型
        src = (PROJECT / "packs" / "docgen" / "pipelines" / "kb-docgen.yaml").read_text(encoding="utf-8")
        offline = src.replace("embedder: auto", "embedder: hash")
        yaml_path = corpus["tmp"] / "kb-docgen-offline.yaml"
        yaml_path.write_text(offline, encoding="utf-8")
        # 片段与引用它的 YAML 必须同目录：chdir 到 tmp 之后 Scheduler 的相对
        # pipeline_dir 也指向 tmp，漏拷就是"片段不存在"而不是漂移。
        shutil.copy(PROJECT / "packs" / "docgen" / "pipelines" / "_quality-tail.yaml",
                    corpus["tmp"] / "_quality-tail.yaml")

        from loom_core import PipelineOrchestrator
        from loom_core.scheduler import Scheduler
        orch = PipelineOrchestrator(
            agents_dir=str(PROJECT / "agents"),
            checkpoint_dir=str(corpus["tmp"] / "checkpoints"))
        try:
            orch.register_agents()
            plan = Scheduler().parse_file(str(yaml_path), verify_lock=False)
            # 只清空 LLM 凭据（不能用 clear=True：那会连 PATH/TMP 一起清掉，
            # Windows 上的 asyncio/tempfile 行为随之失真）
            with patch.dict("os.environ",
                            {"LLM_API_KEY": "", "SILICONFLOW_API_KEY": ""},
                            clear=False), \
                    patch("loom_core.llm_router.get_router", return_value=None):
                # 必须用唯一 task_id：幂等键形如 `{task_id}:{node}:{attempts}`，
                # 而 bus_data/message_bus.db 是按源码位置算出的**绝对路径**
                # （message_store.py:28），同一份 checkout 里重复用同一个
                # task_id 会命中历史键 → request() 返回 None → 节点被记为
                # success 却什么都没做（见 P0-4 的静默绿修复）。
                run_id = f"kb-e2e-{uuid.uuid4().hex[:8]}"
                task = orch.run_plan(plan, input_file=str(corpus["input"]),
                                     wait=True, task_id=run_id)
                steps = [(s.step_name, s.status, (s.error or "")[:120])
                         for s in (task.steps or [])]
                status = getattr(task.status, "value", str(task.status))
                assert status in ("done", "completed"), (status, task.error, steps)

                kb_result = (task.result or {}).get("knowledge_base") or {}
                assert kb_result.get("hits", 0) > 0, (
                    f"knowledge_base 节点没有产出命中（接线断在总线/动作层）：{steps}")

                out = Path(f"output/{run_id}_result.md")
                assert out.exists(), (
                    f"safe_writer 未落盘；steps={steps}; "
                    f"output/={sorted(str(p) for p in Path('output').rglob('*'))}")
                text = out.read_text(encoding="utf-8")
                assert "producer_byte_rate" in text, f"文档未接地到本地资料:\n{text[:400]}"
                assert "未采集到可整合的搜索结果" not in text
        finally:
            orch.shutdown()


# ─── 7. 嵌入后端：auto 不得为了探测而下载模型 ─────────────

class TestEmbedderAutoProbeIsOffline:
    """实测缺陷：`embedder: auto` 触发 SentenceTransformer 构造，
    huggingface.co 不可达时按 1/2/4/8/16s 退避重试 5 次，CLI 跑 kb-docgen
    直接表现为挂死。修法分两层：先看本地有没有缓存（没缓存就根本不构造），
    真要保证构造也不联网则套上离线环境。
    """

    def test_uncached_model_skips_construction_entirely(self, monkeypatch):
        """离线模式不足以止血：ST 仍会查 Hub revision，断网时退避重试数分钟。

        所以 auto 必须先看"本地有没有缓存"，没有就**根本不构造**，
        而不是把希望寄托在 HF_HUB_OFFLINE 上。
        """
        from artesian import embeddings as emb

        calls = {"constructed": 0}

        class _NeverBuild:
            def __init__(self, model=""):
                calls["constructed"] += 1
                raise ValueError("模拟断网重试")

        monkeypatch.setattr(emb, "LocalEmbedder", _NeverBuild)
        monkeypatch.setattr(emb, "available_embedders", lambda: ["hash", "local"])
        monkeypatch.setattr(emb, "model_is_cached", lambda model="": False)
        monkeypatch.delenv("HF_HUB_OFFLINE", raising=False)

        e = emb.get_embedder("auto")
        assert e.name.startswith("hash")
        assert calls["constructed"] == 0, "未缓存时不该尝试构造（那次构造会联网重试）"
        assert "未缓存" in emb.auto_fallback_reasons().get("local", "")

    def test_cached_model_still_probes_under_offline_env(self, monkeypatch):
        from artesian import embeddings as emb

        seen = {}

        class _FakeLocal:
            def __init__(self, model=""):
                seen["offline"] = os.environ.get("HF_HUB_OFFLINE")
                raise ValueError("构造失败")

        monkeypatch.setattr(emb, "LocalEmbedder", _FakeLocal)
        monkeypatch.setattr(emb, "available_embedders", lambda: ["hash", "local"])
        monkeypatch.setattr(emb, "model_is_cached", lambda model="": True)
        monkeypatch.delenv("HF_HUB_OFFLINE", raising=False)

        assert emb.get_embedder("auto").name.startswith("hash")
        assert seen.get("offline") == "1"
        assert "HF_HUB_OFFLINE" not in os.environ, "探测不得把离线环境变量泄漏给调用方"

    def test_explicit_local_stays_online(self, monkeypatch):
        """显式 embedder: local 是用户的选择，允许联网下载。"""
        from artesian import embeddings as emb

        seen = {}

        class _FakeLocal:
            def __init__(self, model=""):
                seen["offline"] = os.environ.get("HF_HUB_OFFLINE")
                raise ValueError("nope")

        monkeypatch.setattr(emb, "LocalEmbedder", _FakeLocal)
        monkeypatch.delenv("HF_HUB_OFFLINE", raising=False)
        with pytest.raises(ValueError):
            emb.get_embedder("local")
        assert seen.get("offline") is None, "显式 local 不应被强制离线"


# ─── 8. 总线契约：DAG 节点必须能被寻址 ────────────────────

class TestBusAddressing:
    """`execute_node_from_scheduler` 用 `f"{agent}.input"` 做 RPC topic。

    Agent 若没订阅自己的 `<name>.input`，request 静默返回 {}（不报错），
    节点看起来"success"却什么都没做 —— knowledge_base 就是这样被埋了两周。
    """

    def _agents_dir(self):
        return str(PROJECT / "agents")

    def test_no_agent_hijacks_another_agents_input_topic(self):
        """反向的一半：A 不得订阅 `B.input`。

        实测事故：ingest 的 INPUT_TOPICS 里多挂了一个 `researcher.input`，
        于是 `bus.request(topic="researcher.input", to_a="researcher")` 被
        ingest 接走并回了 `{"status":"error","message":"未指定待摄入文件"}`。
        引擎把这份回执当成 researcher 的产物记录下来 → docgen 全线拿到空
        results → writer 吐占位文。定向 RPC 的 topic 是排他的，多一个订阅者
        就是改路由，必须由测试钉住。
        """
        from loom_core import PipelineOrchestrator
        orch = PipelineOrchestrator(agents_dir=self._agents_dir(),
                                    checkpoint_dir=str(Path(".pytest_tmp") / "addr2"))
        try:
            orch.register_agents()
            names = {m["name"] for m in orch.registry.list()}
            hijack = []
            for m in orch.registry.list():
                for topic in (m.get("input_topics") or []):
                    if topic.endswith(".input"):
                        owner = topic[: -len(".input")]
                        if owner in names and owner != m["name"]:
                            hijack.append(f"{m['name']} 订阅了 {topic}（属于 {owner}）")
            assert hijack == [], "输入主题被跨 Agent 抢占:\n" + "\n".join(hijack)
        finally:
            orch.shutdown()

    def test_every_pipeline_node_listens_on_its_input_topic(self):
        from loom_core import PipelineOrchestrator
        from loom_core.naming import agent_of
        from loom_core.scheduler import Scheduler
        orch = PipelineOrchestrator(agents_dir=self._agents_dir(),
                                    checkpoint_dir=str(Path(".pytest_tmp") / "addr"))
        try:
            orch.register_agents()
            missing = []
            for yaml_path in sorted((PROJECT / "pipelines").glob("*.yaml")):
                plan = Scheduler().parse_file(str(yaml_path), verify_lock=False)
                for level in plan.levels:
                    for node in level:
                        # 必须走 naming.agent_of：内联片段带来的节点叫
                        # `quality_gate__quality_tail`，只剥 `_pool_` 的话这条护栏
                        # 会在每次抽取时全线误报——而误报的护栏下一步就是被删。
                        base = agent_of(node.agent_name)
                        meta = orch.registry.get_meta(base)
                        topics = list(getattr(meta, "input_topics", []) or [])
                        if f"{base}.input" not in topics:
                            missing.append((yaml_path.name, node.agent_name, topics))
            assert not missing, (
                "以下流水线节点的 Agent 未订阅 <agent>.input，RPC 会静默空转: "
                f"{missing}")
        finally:
            orch.shutdown()
