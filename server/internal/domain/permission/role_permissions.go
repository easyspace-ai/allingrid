package permission

// RolePermissions 角色权限映射
var RolePermissions = map[Role]map[Action]bool{
	// 空间所有者 - 拥有所有权限
	RoleOwner: {
		// 空间权限
		ActionSpaceCreate:      true,
		ActionSpaceDelete:      true,
		ActionSpaceRead:        true,
		ActionSpaceUpdate:      true,
		ActionSpaceInviteEmail: true,
		ActionSpaceInviteLink:  true,
		ActionSpaceGrantRole:   true,

		// 基础表权限
		ActionBaseCreate:                true,
		ActionBaseDelete:                true,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                true,
		ActionBaseInviteEmail:           true,
		ActionBaseInviteLink:            true,
		ActionBaseTableImport:           true,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: true,
		ActionBaseDBConnection:          true,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      true,
		ActionTableRead:        true,
		ActionTableDelete:      true,
		ActionTableUpdate:      true,
		ActionTableImport:      true,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: true,
		ActionTableTrashReset:  true,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: true,
		ActionViewDelete: true,
		ActionViewRead:   true,
		ActionViewUpdate: true,
		ActionViewShare:  true,

		// 字段权限
		ActionFieldCreate: true,
		ActionFieldDelete: true,
		ActionFieldRead:   true,
		ActionFieldUpdate: true,

		// 记录权限
		ActionRecordCreate:  true,
		ActionRecordComment: true,
		ActionRecordDelete:  true,
		ActionRecordRead:    true,
		ActionRecordUpdate:  true,

		// 自动化权限
		ActionAutomationCreate: true,
		ActionAutomationDelete: true,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: true,

		// 用户权限
		ActionUserEmailRead: true,

		// 实例权限
		ActionInstanceRead:   false,
		ActionInstanceUpdate: false,

		// 企业权限
		ActionEnterpriseRead:   false,
		ActionEnterpriseUpdate: false,
	},

	// 创建者 - 大部分权限，但不能删除空间
	RoleCreator: {
		// 空间权限
		ActionSpaceCreate:      false,
		ActionSpaceDelete:      false,
		ActionSpaceRead:        true,
		ActionSpaceUpdate:      true,
		ActionSpaceInviteEmail: true,
		ActionSpaceInviteLink:  true,
		ActionSpaceGrantRole:   false,

		// 基础表权限
		ActionBaseCreate:                true,
		ActionBaseDelete:                true,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                true,
		ActionBaseInviteEmail:           true,
		ActionBaseInviteLink:            true,
		ActionBaseTableImport:           true,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: true,
		ActionBaseDBConnection:          true,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      true,
		ActionTableRead:        true,
		ActionTableDelete:      true,
		ActionTableUpdate:      true,
		ActionTableImport:      true,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: true,
		ActionTableTrashReset:  true,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: true,
		ActionViewDelete: true,
		ActionViewRead:   true,
		ActionViewUpdate: true,
		ActionViewShare:  true,

		// 字段权限
		ActionFieldCreate: true,
		ActionFieldDelete: true,
		ActionFieldRead:   true,
		ActionFieldUpdate: true,

		// 记录权限
		ActionRecordCreate:  true,
		ActionRecordComment: true,
		ActionRecordDelete:  true,
		ActionRecordRead:    true,
		ActionRecordUpdate:  true,

		// 自动化权限
		ActionAutomationCreate: true,
		ActionAutomationDelete: true,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: true,

		// 用户权限
		ActionUserEmailRead: true,

		// 实例权限
		ActionInstanceRead:   false,
		ActionInstanceUpdate: false,

		// 企业权限
		ActionEnterpriseRead:   false,
		ActionEnterpriseUpdate: false,
	},

	// 编辑者 - 可以编辑内容，但不能管理结构
	RoleEditor: {
		// 空间权限
		ActionSpaceCreate:      false,
		ActionSpaceDelete:      false,
		ActionSpaceRead:        true,
		ActionSpaceUpdate:      false,
		ActionSpaceInviteEmail: false,
		ActionSpaceInviteLink:  false,
		ActionSpaceGrantRole:   false,

		// 基础表权限
		ActionBaseCreate:                false,
		ActionBaseDelete:                false,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                false,
		ActionBaseInviteEmail:           false,
		ActionBaseInviteLink:            false,
		ActionBaseTableImport:           false,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: false,
		ActionBaseDBConnection:          false,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      false,
		ActionTableRead:        true,
		ActionTableDelete:      false,
		ActionTableUpdate:      false,
		ActionTableImport:      false,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: false,
		ActionTableTrashReset:  false,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: false,
		ActionViewDelete: false,
		ActionViewRead:   true,
		ActionViewUpdate: false,
		ActionViewShare:  false,

		// 字段权限
		ActionFieldCreate: false,
		ActionFieldDelete: false,
		ActionFieldRead:   true,
		ActionFieldUpdate: false,

		// 记录权限
		ActionRecordCreate:  true,
		ActionRecordComment: true,
		ActionRecordDelete:  true,
		ActionRecordRead:    true,
		ActionRecordUpdate:  true,

		// 自动化权限
		ActionAutomationCreate: false,
		ActionAutomationDelete: false,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: false,

		// 用户权限
		ActionUserEmailRead: false,

		// 实例权限
		ActionInstanceRead:   false,
		ActionInstanceUpdate: false,

		// 企业权限
		ActionEnterpriseRead:   false,
		ActionEnterpriseUpdate: false,
	},

	// 查看者 - 只能查看
	RoleViewer: {
		// 空间权限
		ActionSpaceCreate:      false,
		ActionSpaceDelete:      false,
		ActionSpaceRead:        true,
		ActionSpaceUpdate:      false,
		ActionSpaceInviteEmail: false,
		ActionSpaceInviteLink:  false,
		ActionSpaceGrantRole:   false,

		// 基础表权限
		ActionBaseCreate:                false,
		ActionBaseDelete:                false,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                false,
		ActionBaseInviteEmail:           false,
		ActionBaseInviteLink:            false,
		ActionBaseTableImport:           false,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: false,
		ActionBaseDBConnection:          false,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      false,
		ActionTableRead:        true,
		ActionTableDelete:      false,
		ActionTableUpdate:      false,
		ActionTableImport:      false,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: false,
		ActionTableTrashReset:  false,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: false,
		ActionViewDelete: false,
		ActionViewRead:   true,
		ActionViewUpdate: false,
		ActionViewShare:  false,

		// 字段权限
		ActionFieldCreate: false,
		ActionFieldDelete: false,
		ActionFieldRead:   true,
		ActionFieldUpdate: false,

		// 记录权限
		ActionRecordCreate:  false,
		ActionRecordComment: false,
		ActionRecordDelete:  false,
		ActionRecordRead:    true,
		ActionRecordUpdate:  false,

		// 自动化权限
		ActionAutomationCreate: false,
		ActionAutomationDelete: false,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: false,

		// 用户权限
		ActionUserEmailRead: false,

		// 实例权限
		ActionInstanceRead:   false,
		ActionInstanceUpdate: false,

		// 企业权限
		ActionEnterpriseRead:   false,
		ActionEnterpriseUpdate: false,
	},

	// 基础表所有者 - 拥有基础表的所有权限
	RoleBaseOwner: {
		// 基础表权限
		ActionBaseCreate:                true,
		ActionBaseDelete:                true,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                true,
		ActionBaseInviteEmail:           true,
		ActionBaseInviteLink:            true,
		ActionBaseTableImport:           true,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: true,
		ActionBaseDBConnection:          true,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      true,
		ActionTableRead:        true,
		ActionTableDelete:      true,
		ActionTableUpdate:      true,
		ActionTableImport:      true,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: true,
		ActionTableTrashReset:  true,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: true,
		ActionViewDelete: true,
		ActionViewRead:   true,
		ActionViewUpdate: true,
		ActionViewShare:  true,

		// 字段权限
		ActionFieldCreate: true,
		ActionFieldDelete: true,
		ActionFieldRead:   true,
		ActionFieldUpdate: true,

		// 记录权限
		ActionRecordCreate:  true,
		ActionRecordComment: true,
		ActionRecordDelete:  true,
		ActionRecordRead:    true,
		ActionRecordUpdate:  true,

		// 自动化权限
		ActionAutomationCreate: true,
		ActionAutomationDelete: true,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: true,

		// 用户权限
		ActionUserEmailRead: true,
	},

	// 基础表创建者 - 大部分基础表权限
	RoleBaseCreator: {
		// 基础表权限
		ActionBaseCreate:                true,
		ActionBaseDelete:                true,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                true,
		ActionBaseInviteEmail:           true,
		ActionBaseInviteLink:            true,
		ActionBaseTableImport:           true,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: true,
		ActionBaseDBConnection:          true,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      true,
		ActionTableRead:        true,
		ActionTableDelete:      true,
		ActionTableUpdate:      true,
		ActionTableImport:      true,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: true,
		ActionTableTrashReset:  true,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: true,
		ActionViewDelete: true,
		ActionViewRead:   true,
		ActionViewUpdate: true,
		ActionViewShare:  true,

		// 字段权限
		ActionFieldCreate: true,
		ActionFieldDelete: true,
		ActionFieldRead:   true,
		ActionFieldUpdate: true,

		// 记录权限
		ActionRecordCreate:  true,
		ActionRecordComment: true,
		ActionRecordDelete:  true,
		ActionRecordRead:    true,
		ActionRecordUpdate:  true,

		// 自动化权限
		ActionAutomationCreate: true,
		ActionAutomationDelete: true,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: true,

		// 用户权限
		ActionUserEmailRead: true,
	},

	// 基础表编辑者 - 可以编辑基础表内容
	RoleBaseEditor: {
		// 基础表权限
		ActionBaseCreate:                false,
		ActionBaseDelete:                false,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                false,
		ActionBaseInviteEmail:           false,
		ActionBaseInviteLink:            false,
		ActionBaseTableImport:           false,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: false,
		ActionBaseDBConnection:          false,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      false,
		ActionTableRead:        true,
		ActionTableDelete:      false,
		ActionTableUpdate:      false,
		ActionTableImport:      false,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: false,
		ActionTableTrashReset:  false,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: false,
		ActionViewDelete: false,
		ActionViewRead:   true,
		ActionViewUpdate: false,
		ActionViewShare:  false,

		// 字段权限
		ActionFieldCreate: false,
		ActionFieldDelete: false,
		ActionFieldRead:   true,
		ActionFieldUpdate: false,

		// 记录权限
		ActionRecordCreate:  true,
		ActionRecordComment: true,
		ActionRecordDelete:  true,
		ActionRecordRead:    true,
		ActionRecordUpdate:  true,

		// 自动化权限
		ActionAutomationCreate: false,
		ActionAutomationDelete: false,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: false,

		// 用户权限
		ActionUserEmailRead: false,
	},

	// 基础表查看者 - 只能查看基础表
	RoleBaseViewer: {
		// 基础表权限
		ActionBaseCreate:                false,
		ActionBaseDelete:                false,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                false,
		ActionBaseInviteEmail:           false,
		ActionBaseInviteLink:            false,
		ActionBaseTableImport:           false,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: false,
		ActionBaseDBConnection:          false,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      false,
		ActionTableRead:        true,
		ActionTableDelete:      false,
		ActionTableUpdate:      false,
		ActionTableImport:      false,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: false,
		ActionTableTrashReset:  false,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: false,
		ActionViewDelete: false,
		ActionViewRead:   true,
		ActionViewUpdate: false,
		ActionViewShare:  false,

		// 字段权限
		ActionFieldCreate: false,
		ActionFieldDelete: false,
		ActionFieldRead:   true,
		ActionFieldUpdate: false,

		// 记录权限
		ActionRecordCreate:  false,
		ActionRecordComment: false,
		ActionRecordDelete:  false,
		ActionRecordRead:    true,
		ActionRecordUpdate:  false,

		// 自动化权限
		ActionAutomationCreate: false,
		ActionAutomationDelete: false,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: false,

		// 用户权限
		ActionUserEmailRead: false,
	},

	// 系统管理员 - 拥有所有权限
	RoleSystem: {
		// 所有权限都为true
		ActionSpaceCreate:               true,
		ActionSpaceDelete:               true,
		ActionSpaceRead:                 true,
		ActionSpaceUpdate:               true,
		ActionSpaceInviteEmail:          true,
		ActionSpaceInviteLink:           true,
		ActionSpaceGrantRole:            true,
		ActionBaseCreate:                true,
		ActionBaseDelete:                true,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                true,
		ActionBaseInviteEmail:           true,
		ActionBaseInviteLink:            true,
		ActionBaseTableImport:           true,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: true,
		ActionBaseDBConnection:          true,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,
		ActionTableCreate:               true,
		ActionTableRead:                 true,
		ActionTableDelete:               true,
		ActionTableUpdate:               true,
		ActionTableImport:               true,
		ActionTableExport:               true,
		ActionTableTrashRead:            true,
		ActionTableTrashUpdate:          true,
		ActionTableTrashReset:           true,
		ActionTableRecordHistoryRead:    true,
		ActionViewCreate:                true,
		ActionViewDelete:                true,
		ActionViewRead:                  true,
		ActionViewUpdate:                true,
		ActionViewShare:                 true,
		ActionFieldCreate:               true,
		ActionFieldDelete:               true,
		ActionFieldRead:                 true,
		ActionFieldUpdate:               true,
		ActionRecordCreate:              true,
		ActionRecordComment:             true,
		ActionRecordDelete:              true,
		ActionRecordRead:                true,
		ActionRecordUpdate:              true,
		ActionAutomationCreate:          true,
		ActionAutomationDelete:          true,
		ActionAutomationRead:            true,
		ActionAutomationUpdate:          true,
		ActionUserEmailRead:             true,
		ActionInstanceRead:              true,
		ActionInstanceUpdate:            true,
		ActionEnterpriseRead:            true,
		ActionEnterpriseUpdate:          true,
	},

	// 管理员 - 拥有大部分权限
	RoleAdmin: {
		// 空间权限
		ActionSpaceCreate:      true,
		ActionSpaceDelete:      true,
		ActionSpaceRead:        true,
		ActionSpaceUpdate:      true,
		ActionSpaceInviteEmail: true,
		ActionSpaceInviteLink:  true,
		ActionSpaceGrantRole:   true,

		// 基础表权限
		ActionBaseCreate:                true,
		ActionBaseDelete:                true,
		ActionBaseRead:                  true,
		ActionBaseUpdate:                true,
		ActionBaseInviteEmail:           true,
		ActionBaseInviteLink:            true,
		ActionBaseTableImport:           true,
		ActionBaseTableExport:           true,
		ActionBaseAuthorityMatrixConfig: true,
		ActionBaseDBConnection:          true,
		ActionBaseQueryData:             true,
		ActionBaseReadAll:               true,

		// 表格权限
		ActionTableCreate:      true,
		ActionTableRead:        true,
		ActionTableDelete:      true,
		ActionTableUpdate:      true,
		ActionTableImport:      true,
		ActionTableExport:      true,
		ActionTableTrashRead:   true,
		ActionTableTrashUpdate: true,
		ActionTableTrashReset:  true,

		// 表格记录历史权限
		ActionTableRecordHistoryRead: true,

		// 视图权限
		ActionViewCreate: true,
		ActionViewDelete: true,
		ActionViewRead:   true,
		ActionViewUpdate: true,
		ActionViewShare:  true,

		// 字段权限
		ActionFieldCreate: true,
		ActionFieldDelete: true,
		ActionFieldRead:   true,
		ActionFieldUpdate: true,

		// 记录权限
		ActionRecordCreate:  true,
		ActionRecordComment: true,
		ActionRecordDelete:  true,
		ActionRecordRead:    true,
		ActionRecordUpdate:  true,

		// 自动化权限
		ActionAutomationCreate: true,
		ActionAutomationDelete: true,
		ActionAutomationRead:   true,
		ActionAutomationUpdate: true,

		// 用户权限
		ActionUserEmailRead: true,

		// 实例权限
		ActionInstanceRead:   false,
		ActionInstanceUpdate: false,

		// 企业权限
		ActionEnterpriseRead:   false,
		ActionEnterpriseUpdate: false,
	},
}

// HasRolePermission 检查角色是否有指定权限
func HasRolePermission(role Role, action Action) bool {
	rolePermissions, exists := RolePermissions[role]
	if !exists {
		return false
	}

	hasPermission, exists := rolePermissions[action]
	return exists && hasPermission
}

// GetRolePermissions 获取角色的所有权限
func GetRolePermissions(role Role) []Action {
	rolePermissions, exists := RolePermissions[role]
	if !exists {
		return []Action{}
	}

	var permissions []Action
	for action, hasPermission := range rolePermissions {
		if hasPermission {
			permissions = append(permissions, action)
		}
	}

	return permissions
}

// CheckPermissions 检查角色是否有指定权限列表中的所有权限
func CheckPermissions(role Role, actions []Action) bool {
	for _, action := range actions {
		if !HasRolePermission(role, action) {
			return false
		}
	}
	return true
}

// GetRoleLevel 获取角色级别（数字越大权限越高）
func GetRoleLevel(role Role) int {
	roleLevels := map[Role]int{
		RoleViewer:      1,
		RoleEditor:      2,
		RoleCreator:     3,
		RoleOwner:       4,
		RoleBaseViewer:  1,
		RoleBaseEditor:  2,
		RoleBaseCreator: 3,
		RoleBaseOwner:   4,
		RoleAdmin:       5,
		RoleSystem:      6,
	}

	level, exists := roleLevels[role]
	if !exists {
		return 0
	}
	return level
}

// CanGrantRole 检查是否可以授予指定角色
func CanGrantRole(granterRole, targetRole Role) bool {
	granterLevel := GetRoleLevel(granterRole)
	targetLevel := GetRoleLevel(targetRole)

	// 只能授予比自己级别低的角色
	return granterLevel > targetLevel
}

// IsHigherRole 检查角色A是否比角色B权限更高
func IsHigherRole(roleA, roleB Role) bool {
	return GetRoleLevel(roleA) > GetRoleLevel(roleB)
}

// IsLowerRole 检查角色A是否比角色B权限更低
func IsLowerRole(roleA, roleB Role) bool {
	return GetRoleLevel(roleA) < GetRoleLevel(roleB)
}

// IsEqualRole 检查两个角色是否权限相等
func IsEqualRole(roleA, roleB Role) bool {
	return GetRoleLevel(roleA) == GetRoleLevel(roleB)
}
