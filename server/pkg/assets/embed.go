package assets

import (
	"embed"
)

// StaticFiles 嵌入的静态文件
// 这会在编译时将 web 目录的所有文件嵌入到二进制文件中
// 路径是相对于此文件所在目录的
//
//go:embed web
var StaticFiles embed.FS
