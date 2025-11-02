"use client";

import * as React from "react";
import { useForm, type ControllerRenderProps } from "react-hook-form";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { Field } from "luckdb-sdk";

interface AddRecordDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  fields: Field[];
  onSubmit: (data: Record<string, unknown>) => Promise<void>;
}

export function AddRecordDialog({
  open,
  onOpenChange,
  fields,
  onSubmit,
}: AddRecordDialogProps) {
  // 构建表单默认值
  const defaultValues = React.useMemo(() => {
    const values: Record<string, unknown> = {};
    fields.forEach((field) => {
      if (field.defaultValue !== undefined) {
        values[field.id] = field.defaultValue;
      } else if (field.type === "checkbox" || field.type === "boolean") {
        values[field.id] = false;
      } else {
        values[field.id] = "";
      }
    });
    return values;
  }, [fields]);

  const form = useForm({
    defaultValues,
    mode: "onChange",
  });

  const handleSubmit = async (data: Record<string, unknown>) => {
    try {
      await onSubmit(data);
      form.reset(defaultValues);
      onOpenChange(false);
    } catch (error) {
      console.error("提交表单失败:", error);
    }
  };

  // 当对话框关闭时重置表单
  React.useEffect(() => {
    if (!open) {
      form.reset(defaultValues);
    }
  }, [open, form, defaultValues]);

  // 渲染表单字段
  const renderField = (field: Field) => {
    const fieldType = field.type;

    // 跳过公式字段和 AI 字段（这些字段是自动计算的）
    if (fieldType === "formula" || fieldType === "ai") {
      return null;
    }

    return (
      <FormField
        key={field.id}
        control={form.control}
        name={field.id}
        rules={{
          required: field.required ? `${field.name} 是必填项` : false,
        }}
        render={({ field: formField }) => (
          <FormItem>
            <FormLabel>
              {field.name}
              {field.required && <span className="text-destructive ml-1">*</span>}
            </FormLabel>
            <FormControl>
              {renderFieldInput(field, formField)}
            </FormControl>
            <FormMessage />
          </FormItem>
        )}
      />
    );
  };

  // 根据字段类型渲染输入组件
  const renderFieldInput = (
    field: Field,
    formField: ControllerRenderProps<Record<string, unknown>, string>
  ) => {
    const fieldType = field.type;
    const options = field.options as { choices?: Array<{ id: string; name: string }> };

    switch (fieldType) {
      case "singleLineText":
      case "text":
      case "email":
      case "url":
      case "link":
      case "phone":
        return (
          <Input
            type={fieldType === "email" ? "email" : fieldType === "phone" ? "tel" : "text"}
            {...formField}
            placeholder={`请输入${field.name}`}
          />
        );

      case "longText":
        return (
          <Textarea
            {...formField}
            placeholder={`请输入${field.name}`}
            rows={4}
          />
        );

      case "number":
      case "currency":
      case "percent":
        return (
          <Input
            type="number"
            {...formField}
            placeholder={`请输入${field.name}`}
            onChange={(e) => {
              const value = e.target.value === "" ? "" : Number(e.target.value);
              formField.onChange(value);
            }}
            value={formField.value === "" ? "" : formField.value}
          />
        );

      case "checkbox":
      case "boolean":
        return (
          <div className="flex items-center space-x-2">
            <Checkbox
              checked={formField.value || false}
              onCheckedChange={(checked) => formField.onChange(checked)}
            />
            <label className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70">
              {field.name}
            </label>
          </div>
        );

      case "singleSelect":
      case "select":
        const choices = options?.choices || [];
        return (
          <Select
            value={formField.value || ""}
            onValueChange={formField.onChange}
          >
            <SelectTrigger>
              <SelectValue placeholder={`请选择${field.name}`} />
            </SelectTrigger>
            <SelectContent>
              {choices.map((choice) => (
                <SelectItem key={choice.id} value={choice.id}>
                  {choice.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        );

      case "date":
      case "datetime":
        return (
          <Input
            type={fieldType === "datetime" ? "datetime-local" : "date"}
            {...formField}
            placeholder={`请选择${field.name}`}
          />
        );

      default:
        return (
          <Input
            type="text"
            {...formField}
            placeholder={`请输入${field.name}`}
          />
        );
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>添加新记录</DialogTitle>
          <DialogDescription>
            根据表格字段填写表单，带 * 号的字段为必填项
          </DialogDescription>
        </DialogHeader>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(handleSubmit)} className="space-y-4">
            <div className="grid gap-4 py-4">
              {fields
                .filter((field) => {
                  // 过滤掉公式和 AI 字段
                  return field.type !== "formula" && field.type !== "ai";
                })
                .map(renderField)}
            </div>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => onOpenChange(false)}
              >
                取消
              </Button>
              <Button type="submit" disabled={form.formState.isSubmitting}>
                {form.formState.isSubmitting ? "保存中..." : "保存"}
              </Button>
            </DialogFooter>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
}

