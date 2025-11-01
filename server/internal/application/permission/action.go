package permission

// Action 权限动作（参考原 Teable 项目的Action-based模型）
type Action string

// ==================== Space权限动作 ====================
const (
	ActionSpaceRead               Action = "space|read"
	ActionSpaceUpdate             Action = "space|update"
	ActionSpaceDelete             Action = "space|delete"
	ActionSpaceInviteEmail        Action = "space|invite_email"
	ActionSpaceManageCollaborator Action = "space|manage_collaborator"
)

// ==================== Base权限动作 ====================
const (
	ActionBaseRead               Action = "base|read"
	ActionBaseUpdate             Action = "base|update"
	ActionBaseDelete             Action = "base|delete"
	ActionBaseDuplicate          Action = "base|duplicate"
	ActionBaseManageCollaborator Action = "base|manage_collaborator"
	ActionBaseTableCreate        Action = "base|table_create"
	ActionBaseTableImport        Action = "base|table_import"
)

// ==================== Table权限动作 ====================
const (
	ActionTableRead        Action = "table|read"
	ActionTableUpdate      Action = "table|update"
	ActionTableDelete      Action = "table|delete"
	ActionTableExport      Action = "table|export"
	ActionTableFieldCreate Action = "table|field_create"
	ActionTableFieldUpdate Action = "table|field_update"
	ActionTableFieldDelete Action = "table|field_delete"
	ActionTableViewCreate  Action = "table|view_create"
	ActionTableViewUpdate  Action = "table|view_update"
	ActionTableViewDelete  Action = "table|view_delete"
)

// ==================== Record权限动作 ====================
const (
	ActionRecordRead    Action = "record|read"
	ActionRecordCreate  Action = "record|create"
	ActionRecordUpdate  Action = "record|update"
	ActionRecordDelete  Action = "record|delete"
	ActionRecordComment Action = "record|comment"
)

// ==================== View权限动作 ====================
const (
	ActionViewRead      Action = "view|read"
	ActionViewUpdate    Action = "view|update"
	ActionViewDelete    Action = "view|delete"
	ActionViewShare     Action = "view|share"
	ActionViewDuplicate Action = "view|duplicate"
)
