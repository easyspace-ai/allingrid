// Code generated from Formula.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Formula
import "github.com/antlr4-go/antlr/v4"

type BaseFormulaVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseFormulaVisitor) VisitRoot(ctx *RootContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitFieldReferenceCurly(ctx *FieldReferenceCurlyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitUnaryOp(ctx *UnaryOpContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitStringLiteral(ctx *StringLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitBrackets(ctx *BracketsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitBooleanLiteral(ctx *BooleanLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitRightWhitespaceOrComments(ctx *RightWhitespaceOrCommentsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitDecimalLiteral(ctx *DecimalLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitLeftWhitespaceOrComments(ctx *LeftWhitespaceOrCommentsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitFunctionCall(ctx *FunctionCallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitIntegerLiteral(ctx *IntegerLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitBinaryOp(ctx *BinaryOpContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitWs_or_comment(ctx *Ws_or_commentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitField_reference(ctx *Field_referenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitField_reference_curly(ctx *Field_reference_curlyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitFunc_name(ctx *Func_nameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseFormulaVisitor) VisitIdentifier(ctx *IdentifierContext) interface{} {
	return v.VisitChildren(ctx)
}
