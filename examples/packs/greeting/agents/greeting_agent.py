"""
greeting_agent -- greeting pack 的示例 Agent
==========================================
离线确定性模板（无网络、无 Key、无第三方依赖）。
自定义时替换 handle() 的逻辑；AGENT_NAME 必须与 pack.yaml 声明一致。
"""
from loom_core.base_agent import AgentStatus, BaseAgent, Message

SANDBOX_TRUSTED = True
AGENT_NAME = "greeting_agent"
AGENT_VERSION = "0.1.0"
AGENT_DESC = "greeting pack: 示例 agent（离线确定性）"
AGENT_AUTHOR = "loom pack new"
AGENT_PRIORITY = 50
INPUT_TOPICS = ["greeting_agent.input"]
OUTPUT_TOPICS = ["greeting_agent.done"]


class GreetingAgent(BaseAgent):
    """示例 Agent：将输入内容原样回传（占位逻辑，替换为你的业务）。"""

    def handle(self, msg: Message) -> dict | None:
        content = msg.payload.get("content") or ""
        self.report(AgentStatus.RUNNING, "processing")
        self.publish("greeting_agent.done", {
            "task_id": msg.payload.get("task_id", ""),
            "content": content,
            "status": "ok",
        })
        return {"status": "ok", "content": content}
