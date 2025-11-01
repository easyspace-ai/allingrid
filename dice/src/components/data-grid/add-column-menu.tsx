"use client";

import React, { useState, useMemo, useRef, useEffect } from 'react';
import { 
  Search,
  Text, 
  Hash, 
  Calendar, 
  CheckSquare, 
  Image, 
  Link,
  Mail,
  Phone,
  FileText,
  Star,
  Clock,
  User,
  List,
  Check,
  Sparkles,
  Code,
  Bot,
} from 'lucide-react';
import { cn } from '@/lib/utils';

/**
 * 字段类型分类
 */
export type FieldCategory = 
  | 'basic'      // 基础类型
  | 'select'     // 选择类型
  | 'datetime'   // 日期时间
  | 'link'       // 链接类型
  | 'advanced'   // 高级类型
  | 'collab';    // 协作类型

/**
 * 字段类型定义
 */
export interface FieldType {
  id: string;
  name: string;
  icon: React.ComponentType<any>;
  description: string;
  category: FieldCategory;
  color: string;
  popular?: boolean;
  keywords?: string[];
}

/**
 * 分类配置
 */
const categoryConfig: Record<FieldCategory, {
  name: string;
  icon: React.ComponentType<any>;
  color: string;
}> = {
  basic: {
    name: '基础字段',
    icon: FileText,
    color: '#3b82f6',
  },
  select: {
    name: '选择字段',
    icon: List,
    color: '#8b5cf6',
  },
  datetime: {
    name: '日期时间',
    icon: Calendar,
    color: '#06b6d4',
  },
  link: {
    name: '链接字段',
    icon: Link,
    color: '#10b981',
  },
  advanced: {
    name: '高级字段',
    icon: Sparkles,
    color: '#f59e0b',
  },
  collab: {
    name: '协作字段',
    icon: User,
    color: '#ec4899',
  },
};

/**
 * 字段类型列表（精简版，适合菜单显示）
 */
const fieldTypes: FieldType[] = [
  // 基础类型
  {
    id: 'short-text',
    name: '单行文本',
    icon: Text,
    description: '简短的文本内容',
    category: 'basic',
    color: '#3b82f6',
    popular: true,
    keywords: ['文本', 'text', '单行'],
  },
  {
    id: 'long-text',
    name: '长文本',
    icon: FileText,
    description: '多行文本，支持换行',
    category: 'basic',
    color: '#10b981',
    popular: true,
    keywords: ['长文本', 'textarea', '多行'],
  },
  {
    id: 'number',
    name: '数字',
    icon: Hash,
    description: '数值和计算',
    category: 'basic',
    color: '#f59e0b',
    popular: true,
    keywords: ['数字', 'number', '数值'],
  },
  
  // 选择类型
  {
    id: 'select',
    name: '单选',
    icon: CheckSquare,
    description: '从多个选项中选择一个',
    category: 'select',
    color: '#8b5cf6',
    popular: true,
    keywords: ['单选', 'select', '选项'],
  },
  {
    id: 'multi-select',
    name: '多选',
    icon: List,
    description: '可以选择多个选项',
    category: 'select',
    color: '#ec4899',
    popular: true,
    keywords: ['多选', 'multi', '标签'],
  },
  {
    id: 'checkbox',
    name: '复选框',
    icon: Check,
    description: '是/否 二选一',
    category: 'select',
    color: '#84cc16',
    keywords: ['复选框', 'checkbox', '是否'],
  },
  
  // 日期时间
  {
    id: 'date',
    name: '日期',
    icon: Calendar,
    description: '日期和时间',
    category: 'datetime',
    color: '#06b6d4',
    popular: true,
    keywords: ['日期', 'date', '时间'],
  },
  
  // 链接类型
  {
    id: 'link',
    name: '链接',
    icon: Link,
    description: '网址链接',
    category: 'link',
    color: '#6366f1',
    keywords: ['链接', 'url', '网址'],
  },
  {
    id: 'email',
    name: '邮箱',
    icon: Mail,
    description: '电子邮件地址',
    category: 'link',
    color: '#14b8a6',
    keywords: ['邮箱', 'email', '邮件'],
  },
  {
    id: 'phone',
    name: '电话',
    icon: Phone,
    description: '电话号码',
    category: 'link',
    color: '#ef4444',
    keywords: ['电话', 'phone', '手机'],
  },
  
  // 高级类型
  {
    id: 'rating',
    name: '评分',
    icon: Star,
    description: '星级评分',
    category: 'advanced',
    color: '#eab308',
    keywords: ['评分', 'rating', '星级'],
  },
  {
    id: 'formula',
    name: '公式',
    icon: Code,
    description: '基于其他字段的计算公式',
    category: 'advanced',
    color: '#8b5cf6',
    popular: true,
    keywords: ['公式', 'formula', '计算', '表达式'],
  },
  {
    id: 'ai',
    name: 'AI 字段',
    icon: Bot,
    description: '使用 AI 自动生成内容',
    category: 'advanced',
    color: '#10b981',
    popular: true,
    keywords: ['AI', '人工智能', '自动生成', '智能'],
  },
  
  // 协作类型
  {
    id: 'user',
    name: '成员',
    icon: User,
    description: '选择用户或成员',
    category: 'collab',
    color: '#64748b',
    keywords: ['用户', 'user', '成员', '人员'],
  },
  {
    id: 'attachment',
    name: '附件',
    icon: Image,
    description: '上传文件和图片',
    category: 'collab',
    color: '#f97316',
    keywords: ['附件', 'attachment', '文件', '图片'],
  },
];

export interface AddColumnMenuProps {
  isOpen: boolean;
  onClose: () => void;
  // Airtable 两步式：最终确认时回调；若不传则退化为仅选择类型
  onConfirm?: (payload: { type: string; name?: string; options?: any }) => void;
  onSelect?: (fieldType: string) => void;
  triggerRef?: React.RefObject<HTMLElement | null>;
}

/**
 * Airtable 风格的字段添加菜单
 * 
 * 特性：
 * - 智能定位（在触发元素下方显示）
 * - 自动调整位置防止被遮挡
 * - 紧凑的设计，适合菜单显示
 * - 搜索和分类功能
 */
export function AddColumnMenu({ isOpen, onClose, onConfirm, onSelect, triggerRef }: AddColumnMenuProps) {
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedCategory, setSelectedCategory] = useState<FieldCategory | 'all' | 'popular'>('all');
  const [step, setStep] = useState<'select' | 'configure'>('select');
  const [selectedType, setSelectedType] = useState<FieldType | null>(null);
  const [fieldName, setFieldName] = useState('');
  const [fieldOptions, setFieldOptions] = useState<any>({});
  const menuRef = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ top: 0, left: 0, maxHeight: 400 });

  // 筛选后的字段类型
  const filteredFieldTypes = useMemo(() => {
    let result = fieldTypes;
    
    // 按分类筛选
    if (selectedCategory === 'popular') {
      result = result.filter(type => type.popular);
    } else if (selectedCategory !== 'all') {
      result = result.filter(type => type.category === selectedCategory);
    }
    
    // 按搜索词筛选
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase();
      result = result.filter(type => 
        type.name.toLowerCase().includes(query) ||
        type.description.toLowerCase().includes(query) ||
        type.keywords?.some(keyword => keyword.toLowerCase().includes(query))
      );
    }
    
    return result;
  }, [selectedCategory, searchQuery]);

  // 智能定位逻辑（根据不同步骤的宽度动态修正，避免右侧裁切）
  useEffect(() => {
    if (!isOpen || !triggerRef?.current) return;

    const trigger = triggerRef.current;
    const rect = trigger.getBoundingClientRect();
    const viewport = { width: window.innerWidth, height: window.innerHeight };

    let top = rect.bottom + 4; // 优先在下方
    let left = rect.left;      // 与触发元素左侧对齐
    let maxHeight = 400;

    // 动态菜单宽度
    const menuWidth = step === 'select' ? 320 : 420;
    const margin = 8;

    // 横向边界修正：确保 [margin, viewport.width - menuWidth - margin]
    left = Math.min(Math.max(left, margin), viewport.width - menuWidth - margin);

    // 纵向边界：若下方空间不足则转到上方
    const availableBelow = viewport.height - top - margin;
    if (availableBelow < 220) {
      top = rect.top - 4;
      maxHeight = Math.min(400, rect.top - margin);
    } else {
      maxHeight = Math.min(400, availableBelow);
    }

    setPosition({ top, left, maxHeight });
  }, [isOpen, triggerRef, step]);

  // 打开时重置（修复再次打开仍停留在上次第二步的问题）
  useEffect(() => {
    if (isOpen) {
      setStep('select');
      setSelectedType(null);
      setFieldName('');
      setFieldOptions({});
    }
  }, [isOpen]);

  // 处理字段选择：若提供 onConfirm 则进入配置步骤，否则沿用仅选择
  const handleFieldSelect = (fieldType: string) => {
    const type = fieldTypes.find(t => t.id === fieldType) || null;
    setSelectedType(type);
    if (onConfirm) {
      if (fieldType === 'select' || fieldType === 'multi-select') {
        setFieldOptions({ options: [] });
      } else if (fieldType === 'formula') {
        setFieldOptions({ expression: '' });
      } else if (fieldType === 'ai') {
        setFieldOptions({ 
          task: 'custom',
          prompt: '',
          dependencies: [],
          trigger: 'manual',
          cache: true,
        });
      } else {
        setFieldOptions({});
      }
      setStep('configure');
    } else {
      onSelect?.(fieldType);
      onClose();
    }
  };

  // 处理键盘事件
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') {
      onClose();
    }
  };

  // 点击外部关闭
  useEffect(() => {
    if (!isOpen) return;

    const handleClickOutside = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node) &&
          triggerRef?.current && typeof triggerRef.current.contains === 'function' && 
          !triggerRef.current.contains(e.target as Node)) {
        onClose();
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [isOpen, onClose, triggerRef]);

  if (!isOpen) return null;

  return (
    <>
      {/* 背景遮罩 */}
      <div
        className="fixed inset-0 z-50"
        onClick={onClose}
      />

      {/* 菜单主体（两步式）*/}
      <div
        ref={menuRef}
        className={cn(
          "fixed z-[51] flex flex-col overflow-hidden rounded-lg border bg-popover shadow-lg",
          "animate-in fade-in-0 zoom-in-95"
        )}
        style={{
          top: position.top,
          left: position.left,
          width: step === 'select' ? '320px' : '420px',
          maxHeight: position.maxHeight,
        }}
        onKeyDown={handleKeyDown}
        tabIndex={-1}
      >
        {step === 'select' ? (
        <>
        {/* 搜索框 */}
        <div className="border-b p-3">
          <div className="relative">
            <Search 
              size={16} 
              className="absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none"
            />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="搜索字段类型..."
              autoFocus
              className="w-full rounded-md border border-input bg-background px-3 py-2 pl-8 text-sm outline-none transition-colors focus:border-ring focus:ring-2 focus:ring-ring/20"
            />
          </div>
        </div>

        {/* 分类标签 */}
        <div className="flex flex-wrap gap-1 border-b bg-muted/50 p-2">
          {[
            { id: 'all', name: '全部' },
            { id: 'popular', name: '常用' },
            ...Object.entries(categoryConfig).map(([id, config]) => ({
              id: id as FieldCategory,
              name: config.name,
            })),
          ].map((category) => {
            const isSelected = selectedCategory === category.id;
            
            return (
              <button
                key={category.id}
                onClick={() => setSelectedCategory(category.id as any)}
                className={cn(
                  "rounded border px-2 py-1 text-xs font-medium transition-colors whitespace-nowrap",
                  isSelected 
                    ? "border-border bg-background text-foreground" 
                    : "border-transparent text-muted-foreground hover:bg-background"
                )}
              >
                {category.name}
              </button>
            );
          })}
        </div>

        {/* 字段类型列表 */}
        <div className="flex-1 overflow-y-auto p-1">
          {filteredFieldTypes.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-6 text-center text-sm text-muted-foreground">
              <Search size={24} className="mb-2 text-muted-foreground" />
              <div>没有找到匹配的字段类型</div>
            </div>
          ) : (
            <div className="flex flex-col gap-0.5">
              {filteredFieldTypes.map((type) => {
                const IconComponent = type.icon;
                
                return (
                  <button
                    key={type.id}
                    onClick={() => handleFieldSelect(type.id)}
                    className="flex items-center gap-2.5 rounded-md border-0 bg-transparent px-3 py-2 text-left transition-colors hover:bg-accent/50"
                  >
                    {/* 图标 */}
                    <div
                      className="flex h-6 w-6 shrink-0 items-center justify-center rounded"
                      style={{
                        backgroundColor: `${type.color}15`,
                      }}
                    >
                      <IconComponent size={14} style={{ color: type.color }} />
                    </div>

                    {/* 文字信息 */}
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-1 text-sm font-medium text-foreground">
                        {type.name}
                        {type.popular && (
                          <Star size={10} className="fill-yellow-500 text-yellow-500" />
                        )}
                      </div>
                      <div className="text-xs leading-snug text-muted-foreground">
                        {type.description}
                      </div>
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>
        </>
        ) : (
          <>
            {/* 顶部：字段名 + 类型展示 + 返回 */}
            <div className="flex flex-col gap-2 border-b p-3">
              <input
                type="text"
                value={fieldName}
                onChange={(e) => setFieldName(e.target.value)}
                placeholder="字段名称（可选）"
                autoFocus
                className="w-full rounded-md border border-input bg-background px-2.5 py-2 text-sm outline-none transition-colors focus:border-ring focus:ring-2 focus:ring-ring/20"
              />
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <span className="rounded border border-border bg-muted px-2 py-1">{selectedType?.name}</span>
                <button 
                  onClick={() => setStep('select')} 
                  className="ml-auto cursor-pointer bg-transparent border-0 text-xs text-primary hover:underline"
                >
                  更改类型
                </button>
              </div>
            </div>

            {/* 配置区：覆盖单/多选和公式 */}
            <div className="overflow-y-auto p-3" style={{ maxHeight: position.maxHeight - 120 }}>
              {selectedType?.id === 'select' || selectedType?.id === 'multi-select' ? (
                <SelectOptionsEditor value={fieldOptions} onChange={setFieldOptions} />
              ) : selectedType?.id === 'formula' ? (
                <FormulaEditor value={fieldOptions} onChange={setFieldOptions} />
              ) : selectedType?.id === 'ai' ? (
                <AIFieldEditor value={fieldOptions} onChange={setFieldOptions} />
              ) : (
                <div className="text-xs text-muted-foreground">该字段暂无额外配置</div>
              )}
            </div>

            {/* 底部操作 */}
            <div className="sticky bottom-0 flex gap-2 border-t bg-background p-3 justify-end">
              <button 
                onClick={onClose} 
                className="rounded-md border border-input bg-background px-3 py-2 text-sm cursor-pointer text-foreground hover:bg-accent"
              >
                取消
              </button>
              <button 
                onClick={(e) => {
                  e.stopPropagation();
                  
                  if (!selectedType) {
                    return;
                  }
                  
                  const payload = { 
                    type: selectedType.id, 
                    name: fieldName, 
                    options: fieldOptions 
                  };
                  
                  onConfirm?.(payload);
                  onClose();
                }} 
                className="rounded-md border-0 bg-primary px-3 py-2 text-sm cursor-pointer text-primary-foreground hover:bg-primary/90"
              >
                创建字段
              </button>
            </div>
          </>
        )}
      </div>
    </>
  );
}

// 选项编辑器（简化版：单/多选）
function SelectOptionsEditor({ value, onChange }: { value: any; onChange: (val: any) => void }) {
  const options: Array<{ id: string; name: string; color?: string }> = value?.options ?? [];

  const addOption = () => {
    const next = [...options, { id: `${Date.now()}`, name: `选项${options.length + 1}` }];
    onChange({ ...value, options: next });
  };

  const updateName = (index: number, name: string) => {
    const next = options.slice();
    next[index] = { ...next[index], name };
    onChange({ ...value, options: next });
  };

  const remove = (index: number) => {
    const next = options.filter((_, i) => i !== index);
    onChange({ ...value, options: next });
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="mb-1 text-sm font-semibold text-foreground">选项</div>
      {options.length === 0 && (
        <div className="text-xs text-muted-foreground">暂无选项</div>
      )}
      {options.map((opt, idx) => (
        <div key={opt.id} className="flex gap-2 items-center">
          <input
            value={opt.name}
            onChange={(e) => updateName(idx, e.target.value)}
            className="flex-1 rounded-md border border-input bg-background px-2 py-1.5 text-sm outline-none transition-colors focus:border-ring"
          />
          <button 
            onClick={() => remove(idx)} 
            className="rounded-md border border-input bg-background px-2 py-1.5 text-xs cursor-pointer hover:bg-accent"
          >
            删除
          </button>
        </div>
      ))}
      <button 
        onClick={addOption} 
        className="self-start rounded-md border border-input bg-muted px-3 py-2 text-sm cursor-pointer hover:bg-accent"
      >
        + 添加选项
      </button>
    </div>
  );
}

// 公式编辑器
function FormulaEditor({ value, onChange }: { value: any; onChange: (val: any) => void }) {
  const expression = value?.expression ?? '';

  const handleExpressionChange = (newExpression: string) => {
    onChange({ ...value, expression: newExpression });
  };

  return (
    <div className="flex flex-col gap-2">
      <div className="mb-1 text-sm font-semibold text-foreground">公式表达式</div>
      <textarea
        value={expression}
        onChange={(e) => handleExpressionChange(e.target.value)}
        placeholder="输入公式，例如：{字段1} + {字段2} 或 {数量} * {单价}"
        className="w-full min-h-[120px] rounded-md border border-input bg-background px-3 py-2 text-sm font-mono outline-none transition-colors focus:border-ring focus:ring-2 focus:ring-ring/20 resize-none"
        style={{ fontFamily: 'monospace' }}
      />
      <div className="text-xs text-muted-foreground">
        <p>提示：</p>
        <ul className="list-disc list-inside mt-1 space-y-0.5">
          <li>使用 {'{字段名}'} 引用其他字段</li>
          <li>支持基本数学运算：+, -, *, /</li>
          <li>支持字符串连接：+</li>
        </ul>
      </div>
    </div>
  );
}

// AI 字段编辑器
function AIFieldEditor({ value, onChange }: { value: any; onChange: (val: any) => void }) {
  const task = value?.task ?? 'custom';
  const prompt = value?.prompt ?? '';
  const dependencies = value?.dependencies ?? [];
  const trigger = value?.trigger ?? 'manual';
  const cache = value?.cache ?? true;

  const taskOptions = [
    { value: 'generate', label: '生成内容' },
    { value: 'summarize', label: '总结' },
    { value: 'extract', label: '提取信息' },
    { value: 'translate', label: '翻译' },
    { value: 'classify', label: '分类' },
    { value: 'custom', label: '自定义提示' },
  ];

  const triggerOptions = [
    { value: 'manual', label: '手动触发（点击刷新按钮）' },
    { value: 'auto', label: '自动触发（依赖字段变化时）' },
    { value: 'on-create', label: '仅创建时生成' },
  ];

  const handleTaskChange = (newTask: string) => {
    onChange({ ...value, task: newTask });
  };

  const handlePromptChange = (newPrompt: string) => {
    onChange({ ...value, prompt: newPrompt });
  };

  const handleTriggerChange = (newTrigger: string) => {
    onChange({ ...value, trigger: newTrigger });
  };

  const handleCacheChange = (checked: boolean) => {
    onChange({ ...value, cache: checked });
  };

  return (
    <div className="flex flex-col gap-3">
      {/* 任务类型 */}
      <div>
        <div className="mb-2 text-sm font-semibold text-foreground">AI 任务类型</div>
        <div className="flex flex-col gap-1.5">
          {taskOptions.map((option) => (
            <label
              key={option.value}
              className="flex items-center gap-2 cursor-pointer"
            >
              <input
                type="radio"
                name="ai-task"
                value={option.value}
                checked={task === option.value}
                onChange={(e) => handleTaskChange(e.target.value)}
                className="size-4"
              />
              <span className="text-sm">{option.label}</span>
            </label>
          ))}
        </div>
      </div>

      {/* 自定义提示词 */}
      {task === 'custom' && (
        <div>
          <div className="mb-2 text-sm font-semibold text-foreground">自定义提示词</div>
          <textarea
            value={prompt}
            onChange={(e) => handlePromptChange(e.target.value)}
            placeholder="输入提示词，例如：基于 {Trick name} 和 {Score} 生成一段描述"
            className="w-full min-h-[80px] rounded-md border border-input bg-background px-3 py-2 text-sm outline-none transition-colors focus:border-ring focus:ring-2 focus:ring-ring/20 resize-none"
          />
          <div className="mt-1 text-xs text-muted-foreground">
            使用 {'{字段名}'} 引用其他字段的值
          </div>
        </div>
      )}

      {/* 触发模式 */}
      <div>
        <div className="mb-2 text-sm font-semibold text-foreground">触发模式</div>
        <div className="flex flex-col gap-1.5">
          {triggerOptions.map((option) => (
            <label
              key={option.value}
              className="flex items-center gap-2 cursor-pointer"
            >
              <input
                type="radio"
                name="ai-trigger"
                value={option.value}
                checked={trigger === option.value}
                onChange={(e) => handleTriggerChange(e.target.value)}
                className="size-4"
              />
              <span className="text-sm">{option.label}</span>
            </label>
          ))}
        </div>
      </div>

      {/* 缓存选项 */}
      <div>
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={cache}
            onChange={(e) => handleCacheChange(e.target.checked)}
            className="size-4"
          />
          <span className="text-sm">启用缓存（相同输入复用结果）</span>
        </label>
      </div>

      <div className="rounded-md bg-muted/50 p-2 text-xs text-muted-foreground">
        <p className="font-semibold mb-1">提示：</p>
        <ul className="list-disc list-inside space-y-0.5">
          <li>演示模式使用模拟 AI，不会调用真实 API</li>
          <li>真实 API 接入后，此处配置会自动生效</li>
          <li>依赖字段需要在创建字段后手动配置</li>
        </ul>
      </div>
    </div>
  );
}

