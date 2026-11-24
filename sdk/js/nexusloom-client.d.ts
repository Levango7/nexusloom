export interface LoomClientOptions {
  baseUrl?: string;
  token?: string;
  timeoutMs?: number;
  fetchImpl?: typeof fetch;
}

export interface LoomTaskSnapshot {
  id: string;
  status: string;
  pipeline?: string;
  result?: Record<string, unknown>;
  error?: string | null;
  output_path?: string;
  output_content?: string;
  awaiting_approval?: Record<string, unknown> | null;
  approvals?: Array<Record<string, unknown>>;
  artifacts?: Array<Record<string, unknown>>;
  [key: string]: unknown;
}

export interface LoomSubmitOptions {
  query?: string;
  inputs?: Record<string, unknown>;
  pipeline?: string;
  title?: string;
  wait?: boolean;
  output?: string;
}

export interface LoomGenerateOptions extends LoomSubmitOptions {
  timeout?: number;
  pollInterval?: number;
  approvalHandler?: (snapshot: LoomTaskSnapshot) => "approve" | "reject" | Promise<"approve" | "reject">;
}

export class LoomError extends Error {
  status: number;
  body: unknown;
}

export class LoomTimeoutError extends LoomError {}
export class LoomTaskError extends LoomError {
  taskId: string;
  taskStatus: string;
}
export class LoomApprovalRequired extends LoomTaskError {}

export class LoomClient {
  constructor(options?: LoomClientOptions);
  baseUrl: string;
  token: string;
  timeoutMs: number;
  health(): Promise<Record<string, unknown>>;
  submit(options?: LoomSubmitOptions): Promise<Record<string, unknown>>;
  runWorkflow(pipeline: string, inputs: Record<string, unknown>, options?: {
    wait?: boolean;
    output?: string;
  }): Promise<Record<string, unknown>>;
  status(taskId: string): Promise<LoomTaskSnapshot>;
  wait(taskId: string, options?: { timeout?: number; pollInterval?: number }): Promise<LoomTaskSnapshot>;
  generate(options?: LoomGenerateOptions): Promise<LoomTaskSnapshot>;
  artifacts(taskId: string): Promise<Array<Record<string, unknown>>>;
  cancel(taskId: string): Promise<Record<string, unknown>>;
  pause(taskId: string): Promise<Record<string, unknown>>;
  resume(taskId: string): Promise<Record<string, unknown>>;
  approve(taskId: string, note?: string): Promise<Record<string, unknown>>;
  reject(taskId: string, reason?: string): Promise<Record<string, unknown>>;
}

export function createLoomClient(options?: LoomClientOptions): LoomClient;
export function createLoomClientFromEnv(
  env?: Record<string, string | undefined>,
  options?: LoomClientOptions,
): LoomClient;
