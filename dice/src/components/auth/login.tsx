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
  const [email, setEmail] = React.useState("")
  const [password, setPassword] = React.useState("")
  const [isLoading, setIsLoading] = React.useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)

    try {
      const response = await luckdbClient.auth.login(email, password)
      toast.success("登录成功")
      
      if (onLoginSuccess) {
        onLoginSuccess()
      }
    } catch (error: any) {
      toast.error(error.message || "登录失败，请检查邮箱和密码")
    } finally {
      setIsLoading(false)
    }
  }

  // 检查是否已登录
  React.useEffect(() => {
    const checkAuth = async () => {
      try {
        const user = await luckdbClient.auth.getCurrentUser()
        if (user && onLoginSuccess) {
          onLoginSuccess()
        }
      } catch {
        // 未登录，显示登录界面
      }
    }
    checkAuth()
  }, [onLoginSuccess])

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

