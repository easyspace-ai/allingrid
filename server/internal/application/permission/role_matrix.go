package permission

import "github.com/easyspace-ai/luckdb/server/internal/domain/collaborator/entity"

// RolePermissions 角色权限矩阵（参考原 Teable 项目）
// 定义每个角色可以执行的操作
var RolePermissions = map[entity.RoleName][]Action{
	// Owner: 完全控制
	entity.RoleOwner: {
		// Space
		ActionSpaceRead,
		ActionSpaceUpdate,
		ActionSpaceDelete,
		ActionSpaceInviteEmail,
		ActionSpaceManageCollaborator,
		// Base
		ActionBaseRead,
		ActionBaseUpdate,
		ActionBaseDelete,
		ActionBaseDuplicate,
		ActionBaseManageCollaborator,
		ActionBaseTableCreate,
		ActionBaseTableImport,
		// Table
		ActionTableRead,
		ActionTableUpdate,
		ActionTableDelete,
		ActionTableExport,
		ActionTableFieldCreate,
		ActionTableFieldUpdate,
		ActionTableFieldDelete,
		ActionTableViewCreate,
		ActionTableViewUpdate,
		ActionTableViewDelete,
		// Record
		ActionRecordRead,
		ActionRecordCreate,
		ActionRecordUpdate,
		ActionRecordDelete,
		ActionRecordComment,
		// View
		ActionViewRead,
		ActionViewUpdate,
		ActionViewDelete,
		ActionViewShare,
		ActionViewDuplicate,
	},

	// Creator: 可创建内容
	entity.RoleCreator: {
		// Space
		ActionSpaceRead,
		// Base
		ActionBaseRead,
		ActionBaseTableCreate,
		ActionBaseTableImport,
		// Table
		ActionTableRead,
		ActionTableExport,
		ActionTableFieldCreate,
		ActionTableFieldUpdate,
		ActionTableViewCreate,
		ActionTableViewUpdate,
		// Record
		ActionRecordRead,
		ActionRecordCreate,
		ActionRecordUpdate,
		ActionRecordDelete, // Creator可以删除自己创建的记录
		ActionRecordComment,
		// View
		ActionViewRead,
		ActionViewUpdate,
		ActionViewShare,
		ActionViewDuplicate,
	},

	// Editor: 可编辑内容
	entity.RoleEditor: {
		// Space
		ActionSpaceRead,
		// Base
		ActionBaseRead,
		// Table
		ActionTableRead,
		ActionTableExport,
		ActionTableViewCreate, // Editor可以创建个人视图
		ActionTableViewUpdate,
		// Record
		ActionRecordRead,
		ActionRecordCreate,
		ActionRecordUpdate,
		ActionRecordDelete,
		ActionRecordComment,
		// View
		ActionViewRead,
		ActionViewUpdate,
		ActionViewDuplicate,
	},

	// Viewer: 只读
	entity.RoleViewer: {
		// Space
		ActionSpaceRead,
		// Base
		ActionBaseRead,
		// Table
		ActionTableRead,
		ActionTableExport,
		// Record
		ActionRecordRead,
		// View
		ActionViewRead,
		ActionViewDuplicate,
	},

	// Commenter: 可查看和评论
	entity.RoleCommenter: {
		// Space
		ActionSpaceRead,
		// Base
		ActionBaseRead,
		// Table
		ActionTableRead,
		// Record
		ActionRecordRead,
		ActionRecordComment,
		// View
		ActionViewRead,
	},
}

// HasPermission 检查角色是否有某个权限
func HasPermission(role entity.RoleName, action Action) bool {
	permissions, ok := RolePermissions[role]
	if !ok {
		return false
	}

	for _, perm := range permissions {
		if perm == action {
			return true
		}
	}
	return false
}

// GetRoleActions 获取角色的所有权限动作
func GetRoleActions(role entity.RoleName) []Action {
	return RolePermissions[role]
}
