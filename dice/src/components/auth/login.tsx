"use client"

import * as React from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { luckdbClient } from "@/config/client"
import { toast } from "sonner"

interface LoginProps {
  onLoginSuccess?: () => void
}

export function Login({ onLoginSuccess }: LoginProps) {
  const [email, setEmail] = React.useState("admin@126.com")
  const [password, setPassword] = React.useState("Pmker123")
  const [isLoading, setIsLoading] = React.useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)

    try {
      const response = await luckdbClient.auth.login(email, password)
      
      console.log("[Login] 登录响应:", {
        hasResponse: !!response,
        hasToken: !!(response?.token),
        hasRecord: !!(response?.record),
        tokenLength: response?.token?.length || 0,
      });
      
      // 验证 token 是否已保存
      const savedToken = luckdbClient.authStore.token;
      console.log("[Login] 保存的 token:", {
        hasToken: !!savedToken,
        tokenLength: savedToken?.length || 0,
        isValid: luckdbClient.authStore.isValid,
      });
      
      // 验证响应是否有效
      if (response && response.token && response.record) {
        toast.success("登录成功")
        
        if (onLoginSuccess) {
          onLoginSuccess()
        }
      } else {
        toast.error("登录失败：响应格式错误")
        console.error("Invalid login response:", response)
      }
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : "登录失败，请检查邮箱和密码"
      toast.error(message)
      console.error("Login error:", error)
      if (error instanceof Error) {
        console.error("Login error details:", {
          message: error.message,
          name: error.name,
          stack: error.stack,
        });
      }
    } finally {
      setIsLoading(false)
    }
  }

  // 使用 useRef 保存 onLoginSuccess 回调，避免因回调变化导致重复调用
  const onLoginSuccessRef = React.useRef(onLoginSuccess)
  React.useEffect(() => {
    onLoginSuccessRef.current = onLoginSuccess
  }, [onLoginSuccess])

  // 检查是否已登录（只在组件挂载时检查一次）
  React.useEffect(() => {
    const checkAuth = async () => {
      try {
        const user = await luckdbClient.auth.getCurrentUser()
        if (user && onLoginSuccessRef.current) {
          onLoginSuccessRef.current()
        }
      } catch {
        // 未登录，显示登录界面
      }
    }
    checkAuth()
  }, []) // 只在组件挂载时执行一次

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-4">
      <Card className="w-full max-w-md">
        <CardHeader>
          <CardTitle>登录 LuckDB</CardTitle>
          <CardDescription>请输入您的邮箱和密码</CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="email">邮箱</Label>
              <Input
                id="email"
                type="email"
                placeholder="user@example.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
                disabled={isLoading}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">密码</Label>
              <Input
                id="password"
                type="password"
                placeholder="••••••••"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                disabled={isLoading}
              />
            </div>
            <Button type="submit" className="w-full" disabled={isLoading}>
              {isLoading ? "登录中..." : "登录"}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}

