/**
 * AI Service 抽象接口
 * 
 * 设计说明：
 * - 演示阶段使用 MockAIService
 * - 真实 API 接入时，创建 OpenAIService、AnthropicService 等实现
 * - 通过配置切换不同的实现
 */

export interface AIRequest {
  prompt: string;
  context?: Record<string, any>;
  model?: string;
  temperature?: number;
  maxTokens?: number;
}

export interface AIResponse {
  content: string;
  usage?: {
    tokens: number;
    cost?: number;
  };
  cached?: boolean;
}

export interface AIStreamChunk {
  content: string;
  done: boolean;
}

export interface AIServiceConfig {
  apiKey?: string;
  model?: string;
  baseURL?: string;
  timeout?: number;
  maxRetries?: number;
}

/**
 * AI Service 抽象接口
 */
export interface IAIService {
  /**
   * 生成内容（同步）
   */
  generate(request: AIRequest): Promise<AIResponse>;

  /**
   * 生成内容（流式）
   */
  generateStream?(request: AIRequest): AsyncGenerator<AIStreamChunk>;

  /**
   * 批量生成
   */
  batchGenerate?(requests: AIRequest[]): Promise<AIResponse[]>;
}

/**
 * Mock AI Service - 用于演示
 * 
 * 特点：
 * - 模拟真实的 API 延迟
 * - 根据不同的 prompt 返回不同的模拟结果
 * - 不调用真实 API，零成本
 */
export class MockAIService implements IAIService {
  private delay: number;
  private cache: Map<string, AIResponse>;

  constructor(config?: AIServiceConfig) {
    this.delay = config?.timeout ?? 1000; // 默认 1 秒延迟
    this.cache = new Map();
  }

  async generate(request: AIRequest): Promise<AIResponse> {
    // 检查缓存
    const cacheKey = this.getCacheKey(request);
    if (this.cache.has(cacheKey)) {
      const cached = this.cache.get(cacheKey)!;
      return { ...cached, cached: true };
    }

    // 模拟网络延迟
    await this.sleep(this.delay + Math.random() * 500);

    // 根据 prompt 生成模拟结果
    const content = this.generateMockContent(request);

    const response: AIResponse = {
      content,
      usage: {
        tokens: Math.floor(Math.random() * 100) + 50,
        cost: 0.001, // 模拟成本
      },
    };

    // 缓存结果
    this.cache.set(cacheKey, response);

    return response;
  }

  /**
   * 生成模拟内容
   */
  private generateMockContent(request: AIRequest): string {
    const { prompt, context } = request;
    const lowerPrompt = prompt.toLowerCase();

    // 根据任务类型返回不同的模拟结果
    if (lowerPrompt.includes("总结") || lowerPrompt.includes("summarize")) {
      return this.generateSummary(context);
    }

    if (lowerPrompt.includes("提取") || lowerPrompt.includes("extract")) {
      return this.generateExtraction(context);
    }

    if (lowerPrompt.includes("翻译") || lowerPrompt.includes("translate")) {
      return this.generateTranslation(context);
    }

    if (lowerPrompt.includes("分类") || lowerPrompt.includes("classify")) {
      return this.generateClassification(context);
    }

    if (lowerPrompt.includes("生成") || lowerPrompt.includes("generate")) {
      return this.generateContent(context);
    }

    // 默认：基于上下文生成内容
    return this.generateDefaultContent(context);
  }

  private generateSummary(context?: Record<string, unknown>): string {
    const contextStr = this.formatContext(context);
    return `这是一段关于"${contextStr}"的总结。该内容包含了关键信息和要点，适合快速了解主要信息。`;
  }

  private generateExtraction(context?: Record<string, unknown>): string {
    const contextStr = this.formatContext(context);
    return `从"${contextStr}"中提取的关键信息：重要数据、关键日期、核心概念等。`;
  }

  private generateTranslation(context?: Record<string, unknown>): string {
    const contextStr = this.formatContext(context);
    return `[翻译] ${contextStr} → Translated content based on context`;
  }

  private generateClassification(context?: Record<string, unknown>): string {
    const categories = ["类别A", "类别B", "类别C"];
    return categories[Math.floor(Math.random() * categories.length)];
  }

  private generateContent(context?: Record<string, unknown>): string {
    const contextStr = this.formatContext(context);
    return `基于"${contextStr}"生成的智能内容。这是一段根据提供的信息自动生成的专业描述。`;
  }

  private generateDefaultContent(context?: Record<string, unknown>): string {
    const contextStr = this.formatContext(context);
    return `AI 生成的内容：${contextStr || "基于上下文智能生成的内容"}`;
  }

  private formatContext(context?: Record<string, unknown>): string {
    if (!context) return "";
    return Object.entries(context)
      .map(([key, value]) => {
        if (value === null || value === undefined) return "";
        return `${key}: ${String(value)}`;
      })
      .filter(Boolean)
      .join(", ");
  }

  private getCacheKey(request: AIRequest): string {
    const { prompt, context } = request;
    const contextStr = JSON.stringify(context || {});
    return `${prompt}:${contextStr}`;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * 清除缓存（用于测试）
   */
  clearCache(): void {
    this.cache.clear();
  }
}

/**
 * AI Service 工厂函数
 * 
 * 使用方式：
 * ```typescript
 * // 演示模式
 * const aiService = createAIService({ mode: 'mock' });
 * 
 * // 真实 API 模式（未来）
 * const aiService = createAIService({ 
 *   mode: 'openai',
 *   apiKey: process.env.OPENAI_API_KEY 
 * });
 * ```
 */
export function createAIService(config?: {
  mode?: "mock" | "openai" | "anthropic";
  config?: AIServiceConfig;
}): IAIService {
  const mode = config?.mode ?? "mock";

  switch (mode) {
    case "mock":
      return new MockAIService(config?.config);
    case "openai":
      // TODO: 实现 OpenAIService
      throw new Error("OpenAI service not implemented yet");
    case "anthropic":
      // TODO: 实现 AnthropicService
      throw new Error("Anthropic service not implemented yet");
    default:
      return new MockAIService(config?.config);
  }
}

/**
 * 默认 AI Service 实例（演示模式）
 */
export const defaultAIService = createAIService({ mode: "mock" });

