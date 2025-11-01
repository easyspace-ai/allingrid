"use client";

import * as React from "react";
import { Plus, Upload, X } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface FileUploadDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onFilesSelected: (files: File[]) => void;
  existingFiles?: Array<{ name: string; url: string }>;
  onFileRemove?: (index: number) => void;
}

export function FileUploadDialog({
  open,
  onOpenChange,
  onFilesSelected,
  existingFiles = [],
  onFileRemove,
}: FileUploadDialogProps) {
  const [isDragging, setIsDragging] = React.useState(false);
  const fileInputRef = React.useRef<HTMLInputElement>(null);
  const dropZoneRef = React.useRef<HTMLDivElement>(null);

  const handleFileSelect = React.useCallback(
    (files: FileList | File[]) => {
      const fileArray = Array.from(files);
      if (fileArray.length > 0) {
        onFilesSelected(fileArray);
      }
    },
    [onFilesSelected],
  );

  const handleInputChange = React.useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const files = event.target.files;
      if (files) {
        handleFileSelect(files);
        // 重置 input，允许重复选择相同文件
        if (fileInputRef.current) {
          fileInputRef.current.value = "";
        }
      }
    },
    [handleFileSelect],
  );

  const handleDragOver = React.useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.stopPropagation();
    setIsDragging(true);
  }, []);

  const handleDragLeave = React.useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.stopPropagation();
    setIsDragging(false);
  }, []);

  const handleDrop = React.useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();
      event.stopPropagation();
      setIsDragging(false);

      const files = event.dataTransfer.files;
      if (files && files.length > 0) {
        handleFileSelect(files);
      }
    },
    [handleFileSelect],
  );

  // 处理粘贴事件
  React.useEffect(() => {
    if (!open) return;

    const handlePaste = (event: ClipboardEvent) => {
      const items = event.clipboardData?.items;
      if (!items) return;

      const files: File[] = [];
      for (let i = 0; i < items.length; i++) {
        const item = items[i];
        if (item.kind === "file") {
          const file = item.getAsFile();
          if (file) {
            files.push(file);
          }
        }
      }

      if (files.length > 0) {
        event.preventDefault();
        handleFileSelect(files);
      }
    };

    document.addEventListener("paste", handlePaste);
    return () => {
      document.removeEventListener("paste", handlePaste);
    };
  }, [open, handleFileSelect]);

  const handleUploadClick = React.useCallback(() => {
    fileInputRef.current?.click();
  }, []);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>上传文件</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          {/* 拖放区域 */}
          <div
            ref={dropZoneRef}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            className={cn(
              "relative flex min-h-[200px] flex-col items-center justify-center rounded-lg border-2 border-dashed transition-colors",
              isDragging
                ? "border-primary bg-primary/10"
                : "border-muted-foreground/25 bg-muted/50",
            )}
          >
            <input
              ref={fileInputRef}
              type="file"
              multiple
              onChange={handleInputChange}
              className="hidden"
              tabIndex={-1}
            />
            <div className="flex flex-col items-center gap-2 px-6 py-8 text-center">
              <Upload className="size-8 text-muted-foreground" />
              <p className="text-sm text-muted-foreground">
                粘贴或拖放文件到此处上传
              </p>
            </div>
          </div>

          {/* 上传按钮 */}
          <Button
            onClick={handleUploadClick}
            className="w-full"
            variant="default"
          >
            <Plus className="mr-2 size-4" />
            上传
          </Button>

          {/* 已上传的文件列表 */}
          {existingFiles.length > 0 && (
            <div className="space-y-2">
              <p className="text-sm font-medium">已上传的文件</p>
              <div className="space-y-1 max-h-[200px] overflow-y-auto">
                {existingFiles.map((file, index) => (
                  <div
                    key={index}
                    className="flex items-center justify-between rounded-md border bg-muted px-3 py-2 text-sm"
                  >
                    <span className="truncate flex-1">{file.name}</span>
                    {onFileRemove && (
                      <button
                        type="button"
                        onClick={() => onFileRemove(index)}
                        className="ml-2 hover:text-destructive"
                      >
                        <X className="size-4" />
                      </button>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}

