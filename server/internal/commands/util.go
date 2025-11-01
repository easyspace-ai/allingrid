package commands

import (
	"fmt"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/bcrypt"

	"github.com/easyspace-ai/luckdb/server/internal/config"
)

// NewUtilCmd 创建工具命令
func NewUtilCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "util",
		Short: "实用工具命令",
		Long:  "各种实用工具命令，包括密码生成、配置调试等",
	}

	cmd.AddCommand(newGeneratePasswordCmd())
	cmd.AddCommand(newDebugConfigCmd(configPath))

	return cmd
}

// newGeneratePasswordCmd 创建密码生成命令
func newGeneratePasswordCmd() *cobra.Command {
	var password string

	cmd := &cobra.Command{
		Use:   "generate-password",
		Short: "生成 bcrypt 密码哈希",
		Long:  "为给定的密码生成 bcrypt 哈希值，用于用户密码存储",
		Example: `  # 生成密码哈希
  luckdb util generate-password --password mypassword123`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if password == "" {
				return fmt.Errorf("--password 参数是必需的")
			}

			hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
			if err != nil {
				return fmt.Errorf("生成密码哈希失败: %w", err)
			}

			fmt.Printf("原始密码: %s\n", password)
			fmt.Printf("BCrypt 哈希: %s\n", string(hash))
			fmt.Printf("\n💡 提示: 将此哈希值保存到数据库中\n")

			return nil
		},
	}

	cmd.Flags().StringVarP(&password, "password", "p", "", "要生成哈希的密码")
	cmd.MarkFlagRequired("password")

	return cmd
}

// newDebugConfigCmd 创建配置调试命令
func newDebugConfigCmd(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug-config",
		Short: "打印当前加载的配置",
		Long:  "调试工具：显示当前加载的配置信息，用于排查配置问题",
		Example: `  # 查看默认配置
  luckdb util debug-config
  
  # 查看指定配置文件
  luckdb util debug-config --config production.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("加载配置失败: %w", err)
			}

			fmt.Println("📋 数据库配置:")
			fmt.Printf("  主机: %s\n", cfg.Database.Host)
			fmt.Printf("  端口: %d\n", cfg.Database.Port)
			fmt.Printf("  用户: %s\n", cfg.Database.User)
			fmt.Printf("  密码: %s\n", maskPassword(cfg.Database.Password))
			fmt.Printf("  数据库名: %s\n", cfg.Database.Name)
			fmt.Printf("  SSL模式: %s\n", cfg.Database.SSLMode)
			fmt.Printf("  DSN: %s\n", maskDSN(cfg.Database.GetDSN()))

			fmt.Println("\n📋 Redis 配置:")
			fmt.Printf("  主机: %s\n", cfg.Redis.Host)
			fmt.Printf("  端口: %d\n", cfg.Redis.Port)
			fmt.Printf("  数据库: %d\n", cfg.Redis.DB)
			fmt.Printf("  密码: %s\n", maskPassword(cfg.Redis.Password))

			fmt.Println("\n📋 服务器配置:")
			fmt.Printf("  端口: %d\n", cfg.Server.Port)
			fmt.Printf("  模式: %s\n", cfg.Server.Mode)

			fmt.Println("\n📋 日志配置:")
			fmt.Printf("  级别: %s\n", cfg.Logger.Level)
			fmt.Printf("  格式: %s\n", cfg.Logger.Format)
			fmt.Printf("  输出: %s\n", cfg.Logger.OutputPath)

			return nil
		},
	}

	return cmd
}

// maskPassword 遮蔽密码
func maskPassword(password string) string {
	if password == "" {
		return "<empty>"
	}
	if len(password) <= 4 {
		return "****"
	}
	return password[:2] + "****" + password[len(password)-2:]
}

// maskDSN 遮蔽DSN中的密码
func maskDSN(dsn string) string {
	// 简单实现：如果包含 password= 就隐藏
	// 实际生产中可以用正则表达式更精确地处理
	if len(dsn) == 0 {
		return dsn
	}
	return dsn[:20] + "...****..." + dsn[len(dsn)-20:]
}
