// Code generated from Formula.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // Formula
import (
	"fmt"
	"strconv"
	"sync"

	"github.com/antlr4-go/antlr/v4"
)

// Suppress unused import errors
var _ = fmt.Printf
var _ = strconv.Itoa
var _ = sync.Once{}

type Formula struct {
	*antlr.BaseParser
}

var FormulaParserStaticData struct {
	once                   sync.Once
	serializedATN          []int32
	LiteralNames           []string
	SymbolicNames          []string
	RuleNames              []string
	PredictionContextCache *antlr.PredictionContextCache
	atn                    *antlr.ATN
	decisionToDFA          []*antlr.DFA
}

func formulaParserInit() {
	staticData := &FormulaParserStaticData
	staticData.LiteralNames = []string{
		"", "", "", "", "", "", "", "','", "':'", "'::'", "'$'", "'$$'", "'*'",
		"'('", "')'", "'['", "']'", "'{'", "'}'", "", "", "", "", "", "'.'",
		"", "", "", "", "", "'&'", "'&&'", "'&<'", "'@@'", "'@>'", "'@'", "'!'",
		"'!!'", "'!='", "'^'", "'='", "'=>'", "'>'", "'>='", "'>>'", "'#'",
		"'#='", "'#>'", "'#>>'", "'##'", "'->'", "'->>'", "'-|-'", "'<'", "'<='",
		"'<@'", "'<^'", "'<>'", "'<->'", "'<<'", "'<<='", "'<?>'", "'-'", "'%'",
		"'|'", "'||'", "'||/'", "'|/'", "'+'", "'?'", "'?&'", "'?#'", "'?-'",
		"'?|'", "'/'", "'~'", "'~='", "'~>=~'", "'~>~'", "'~<=~'", "'~<~'",
		"'~*'", "'~~'", "';'",
	}
	staticData.SymbolicNames = []string{
		"", "BLOCK_COMMENT", "LINE_COMMENT", "WHITESPACE", "TRUE", "FALSE",
		"FIELD", "COMMA", "COLON", "COLON_COLON", "DOLLAR", "DOLLAR_DOLLAR",
		"STAR", "OPEN_PAREN", "CLOSE_PAREN", "OPEN_BRACKET", "CLOSE_BRACKET",
		"L_CURLY", "R_CURLY", "BIT_STRING", "REGEX_STRING", "NUMERIC_LITERAL",
		"INTEGER_LITERAL", "HEX_INTEGER_LITERAL", "DOT", "SINGLEQ_STRING_LITERAL",
		"DOUBLEQ_STRING_LITERAL", "IDENTIFIER_VARIABLE", "IDENTIFIER_UNICODE",
		"IDENTIFIER", "AMP", "AMP_AMP", "AMP_LT", "AT_AT", "AT_GT", "AT_SIGN",
		"BANG", "BANG_BANG", "BANG_EQUAL", "CARET", "EQUAL", "EQUAL_GT", "GT",
		"GTE", "GT_GT", "HASH", "HASH_EQ", "HASH_GT", "HASH_GT_GT", "HASH_HASH",
		"HYPHEN_GT", "HYPHEN_GT_GT", "HYPHEN_PIPE_HYPHEN", "LT", "LTE", "LT_AT",
		"LT_CARET", "LT_GT", "LT_HYPHEN_GT", "LT_LT", "LT_LT_EQ", "LT_QMARK_GT",
		"MINUS", "PERCENT", "PIPE", "PIPE_PIPE", "PIPE_PIPE_SLASH", "PIPE_SLASH",
		"PLUS", "QMARK", "QMARK_AMP", "QMARK_HASH", "QMARK_HYPHEN", "QMARK_PIPE",
		"SLASH", "TIL", "TIL_EQ", "TIL_GTE_TIL", "TIL_GT_TIL", "TIL_LTE_TIL",
		"TIL_LT_TIL", "TIL_STAR", "TIL_TIL", "SEMI", "ErrorCharacter",
	}
	staticData.RuleNames = []string{
		"root", "expr", "ws_or_comment", "field_reference", "field_reference_curly",
		"func_name", "identifier",
	}
	staticData.PredictionContextCache = antlr.NewPredictionContextCache()
	staticData.serializedATN = []int32{
		4, 1, 84, 88, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7,
		4, 2, 5, 7, 5, 2, 6, 7, 6, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 1, 39, 8, 1, 10, 1, 12, 1, 42, 9, 1, 3, 1,
		44, 8, 1, 1, 1, 1, 1, 3, 1, 48, 8, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 5, 1, 73, 8, 1, 10, 1, 12, 1, 76, 9, 1,
		1, 2, 1, 2, 1, 3, 1, 3, 1, 4, 1, 4, 1, 5, 1, 5, 1, 6, 1, 6, 1, 6, 0, 1,
		2, 7, 0, 2, 4, 6, 8, 10, 12, 0, 7, 1, 0, 4, 5, 3, 0, 12, 12, 63, 63, 74,
		74, 2, 0, 62, 62, 68, 68, 2, 0, 42, 43, 53, 54, 2, 0, 38, 38, 40, 40, 1,
		0, 1, 3, 1, 0, 28, 29, 99, 0, 14, 1, 0, 0, 0, 2, 47, 1, 0, 0, 0, 4, 77,
		1, 0, 0, 0, 6, 79, 1, 0, 0, 0, 8, 81, 1, 0, 0, 0, 10, 83, 1, 0, 0, 0, 12,
		85, 1, 0, 0, 0, 14, 15, 3, 2, 1, 0, 15, 16, 5, 0, 0, 1, 16, 1, 1, 0, 0,
		0, 17, 18, 6, 1, -1, 0, 18, 48, 5, 25, 0, 0, 19, 48, 5, 26, 0, 0, 20, 48,
		5, 22, 0, 0, 21, 48, 5, 21, 0, 0, 22, 48, 7, 0, 0, 0, 23, 24, 3, 4, 2,
		0, 24, 25, 3, 2, 1, 13, 25, 48, 1, 0, 0, 0, 26, 27, 5, 13, 0, 0, 27, 28,
		3, 2, 1, 0, 28, 29, 5, 14, 0, 0, 29, 48, 1, 0, 0, 0, 30, 31, 5, 62, 0,
		0, 31, 48, 3, 2, 1, 10, 32, 48, 3, 8, 4, 0, 33, 34, 3, 10, 5, 0, 34, 43,
		5, 13, 0, 0, 35, 40, 3, 2, 1, 0, 36, 37, 5, 7, 0, 0, 37, 39, 3, 2, 1, 0,
		38, 36, 1, 0, 0, 0, 39, 42, 1, 0, 0, 0, 40, 38, 1, 0, 0, 0, 40, 41, 1,
		0, 0, 0, 41, 44, 1, 0, 0, 0, 42, 40, 1, 0, 0, 0, 43, 35, 1, 0, 0, 0, 43,
		44, 1, 0, 0, 0, 44, 45, 1, 0, 0, 0, 45, 46, 5, 14, 0, 0, 46, 48, 1, 0,
		0, 0, 47, 17, 1, 0, 0, 0, 47, 19, 1, 0, 0, 0, 47, 20, 1, 0, 0, 0, 47, 21,
		1, 0, 0, 0, 47, 22, 1, 0, 0, 0, 47, 23, 1, 0, 0, 0, 47, 26, 1, 0, 0, 0,
		47, 30, 1, 0, 0, 0, 47, 32, 1, 0, 0, 0, 47, 33, 1, 0, 0, 0, 48, 74, 1,
		0, 0, 0, 49, 50, 10, 9, 0, 0, 50, 51, 7, 1, 0, 0, 51, 73, 3, 2, 1, 10,
		52, 53, 10, 8, 0, 0, 53, 54, 7, 2, 0, 0, 54, 73, 3, 2, 1, 9, 55, 56, 10,
		7, 0, 0, 56, 57, 7, 3, 0, 0, 57, 73, 3, 2, 1, 8, 58, 59, 10, 6, 0, 0, 59,
		60, 7, 4, 0, 0, 60, 73, 3, 2, 1, 7, 61, 62, 10, 5, 0, 0, 62, 63, 5, 31,
		0, 0, 63, 73, 3, 2, 1, 6, 64, 65, 10, 4, 0, 0, 65, 66, 5, 65, 0, 0, 66,
		73, 3, 2, 1, 5, 67, 68, 10, 3, 0, 0, 68, 69, 5, 30, 0, 0, 69, 73, 3, 2,
		1, 4, 70, 71, 10, 12, 0, 0, 71, 73, 3, 4, 2, 0, 72, 49, 1, 0, 0, 0, 72,
		52, 1, 0, 0, 0, 72, 55, 1, 0, 0, 0, 72, 58, 1, 0, 0, 0, 72, 61, 1, 0, 0,
		0, 72, 64, 1, 0, 0, 0, 72, 67, 1, 0, 0, 0, 72, 70, 1, 0, 0, 0, 73, 76,
		1, 0, 0, 0, 74, 72, 1, 0, 0, 0, 74, 75, 1, 0, 0, 0, 75, 3, 1, 0, 0, 0,
		76, 74, 1, 0, 0, 0, 77, 78, 7, 5, 0, 0, 78, 5, 1, 0, 0, 0, 79, 80, 5, 28,
		0, 0, 80, 7, 1, 0, 0, 0, 81, 82, 5, 27, 0, 0, 82, 9, 1, 0, 0, 0, 83, 84,
		3, 12, 6, 0, 84, 11, 1, 0, 0, 0, 85, 86, 7, 6, 0, 0, 86, 13, 1, 0, 0, 0,
		5, 40, 43, 47, 72, 74,
	}
	deserializer := antlr.NewATNDeserializer(nil)
	staticData.atn = deserializer.Deserialize(staticData.serializedATN)
	atn := staticData.atn
	staticData.decisionToDFA = make([]*antlr.DFA, len(atn.DecisionToState))
	decisionToDFA := staticData.decisionToDFA
	for index, state := range atn.DecisionToState {
		decisionToDFA[index] = antlr.NewDFA(state, index)
	}
}

// FormulaInit initializes any static state used to implement Formula. By default the
// static state used to implement the parser is lazily initialized during the first call to
// NewFormula(). You can call this function if you wish to initialize the static state ahead
// of time.
func FormulaInit() {
	staticData := &FormulaParserStaticData
	staticData.once.Do(formulaParserInit)
}

// NewFormula produces a new parser instance for the optional input antlr.TokenStream.
func NewFormula(input antlr.TokenStream) *Formula {
	FormulaInit()
	this := new(Formula)
	this.BaseParser = antlr.NewBaseParser(input)
	staticData := &FormulaParserStaticData
	this.Interpreter = antlr.NewParserATNSimulator(this, staticData.atn, staticData.decisionToDFA, staticData.PredictionContextCache)
	this.RuleNames = staticData.RuleNames
	this.LiteralNames = staticData.LiteralNames
	this.SymbolicNames = staticData.SymbolicNames
	this.GrammarFileName = "Formula.g4"

	return this
}

// Formula tokens.
const (
	FormulaEOF                    = antlr.TokenEOF
	FormulaBLOCK_COMMENT          = 1
	FormulaLINE_COMMENT           = 2
	FormulaWHITESPACE             = 3
	FormulaTRUE                   = 4
	FormulaFALSE                  = 5
	FormulaFIELD                  = 6
	FormulaCOMMA                  = 7
	FormulaCOLON                  = 8
	FormulaCOLON_COLON            = 9
	FormulaDOLLAR                 = 10
	FormulaDOLLAR_DOLLAR          = 11
	FormulaSTAR                   = 12
	FormulaOPEN_PAREN             = 13
	FormulaCLOSE_PAREN            = 14
	FormulaOPEN_BRACKET           = 15
	FormulaCLOSE_BRACKET          = 16
	FormulaL_CURLY                = 17
	FormulaR_CURLY                = 18
	FormulaBIT_STRING             = 19
	FormulaREGEX_STRING           = 20
	FormulaNUMERIC_LITERAL        = 21
	FormulaINTEGER_LITERAL        = 22
	FormulaHEX_INTEGER_LITERAL    = 23
	FormulaDOT                    = 24
	FormulaSINGLEQ_STRING_LITERAL = 25
	FormulaDOUBLEQ_STRING_LITERAL = 26
	FormulaIDENTIFIER_VARIABLE    = 27
	FormulaIDENTIFIER_UNICODE     = 28
	FormulaIDENTIFIER             = 29
	FormulaAMP                    = 30
	FormulaAMP_AMP                = 31
	FormulaAMP_LT                 = 32
	FormulaAT_AT                  = 33
	FormulaAT_GT                  = 34
	FormulaAT_SIGN                = 35
	FormulaBANG                   = 36
	FormulaBANG_BANG              = 37
	FormulaBANG_EQUAL             = 38
	FormulaCARET                  = 39
	FormulaEQUAL                  = 40
	FormulaEQUAL_GT               = 41
	FormulaGT                     = 42
	FormulaGTE                    = 43
	FormulaGT_GT                  = 44
	FormulaHASH                   = 45
	FormulaHASH_EQ                = 46
	FormulaHASH_GT                = 47
	FormulaHASH_GT_GT             = 48
	FormulaHASH_HASH              = 49
	FormulaHYPHEN_GT              = 50
	FormulaHYPHEN_GT_GT           = 51
	FormulaHYPHEN_PIPE_HYPHEN     = 52
	FormulaLT                     = 53
	FormulaLTE                    = 54
	FormulaLT_AT                  = 55
	FormulaLT_CARET               = 56
	FormulaLT_GT                  = 57
	FormulaLT_HYPHEN_GT           = 58
	FormulaLT_LT                  = 59
	FormulaLT_LT_EQ               = 60
	FormulaLT_QMARK_GT            = 61
	FormulaMINUS                  = 62
	FormulaPERCENT                = 63
	FormulaPIPE                   = 64
	FormulaPIPE_PIPE              = 65
	FormulaPIPE_PIPE_SLASH        = 66
	FormulaPIPE_SLASH             = 67
	FormulaPLUS                   = 68
	FormulaQMARK                  = 69
	FormulaQMARK_AMP              = 70
	FormulaQMARK_HASH             = 71
	FormulaQMARK_HYPHEN           = 72
	FormulaQMARK_PIPE             = 73
	FormulaSLASH                  = 74
	FormulaTIL                    = 75
	FormulaTIL_EQ                 = 76
	FormulaTIL_GTE_TIL            = 77
	FormulaTIL_GT_TIL             = 78
	FormulaTIL_LTE_TIL            = 79
	FormulaTIL_LT_TIL             = 80
	FormulaTIL_STAR               = 81
	FormulaTIL_TIL                = 82
	FormulaSEMI                   = 83
	FormulaErrorCharacter         = 84
)

// Formula rules.
const (
	FormulaRULE_root                  = 0
	FormulaRULE_expr                  = 1
	FormulaRULE_ws_or_comment         = 2
	FormulaRULE_field_reference       = 3
	FormulaRULE_field_reference_curly = 4
	FormulaRULE_func_name             = 5
	FormulaRULE_identifier            = 6
)

// IRootContext is an interface to support dynamic dispatch.
type IRootContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Expr() IExprContext
	EOF() antlr.TerminalNode

	// IsRootContext differentiates from other interfaces.
	IsRootContext()
}

type RootContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyRootContext() *RootContext {
	var p = new(RootContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_root
	return p
}

func InitEmptyRootContext(p *RootContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_root
}

func (*RootContext) IsRootContext() {}

func NewRootContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *RootContext {
	var p = new(RootContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = FormulaRULE_root

	return p
}

func (s *RootContext) GetParser() antlr.Parser { return s.parser }

func (s *RootContext) Expr() IExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *RootContext) EOF() antlr.TerminalNode {
	return s.GetToken(FormulaEOF, 0)
}

func (s *RootContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RootContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *RootContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitRoot(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *Formula) Root() (localctx IRootContext) {
	localctx = NewRootContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 0, FormulaRULE_root)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(14)
		p.expr(0)
	}
	{
		p.SetState(15)
		p.Match(FormulaEOF)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IExprContext is an interface to support dynamic dispatch.
type IExprContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser
	// IsExprContext differentiates from other interfaces.
	IsExprContext()
}

type ExprContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyExprContext() *ExprContext {
	var p = new(ExprContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_expr
	return p
}

func InitEmptyExprContext(p *ExprContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_expr
}

func (*ExprContext) IsExprContext() {}

func NewExprContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *ExprContext {
	var p = new(ExprContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = FormulaRULE_expr

	return p
}

func (s *ExprContext) GetParser() antlr.Parser { return s.parser }

func (s *ExprContext) CopyAll(ctx *ExprContext) {
	s.CopyFrom(&ctx.BaseParserRuleContext)
}

func (s *ExprContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *ExprContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

type FieldReferenceCurlyContext struct {
	ExprContext
}

func NewFieldReferenceCurlyContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *FieldReferenceCurlyContext {
	var p = new(FieldReferenceCurlyContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *FieldReferenceCurlyContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FieldReferenceCurlyContext) Field_reference_curly() IField_reference_curlyContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IField_reference_curlyContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IField_reference_curlyContext)
}

func (s *FieldReferenceCurlyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitFieldReferenceCurly(s)

	default:
		return t.VisitChildren(s)
	}
}

type UnaryOpContext struct {
	ExprContext
}

func NewUnaryOpContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *UnaryOpContext {
	var p = new(UnaryOpContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *UnaryOpContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *UnaryOpContext) MINUS() antlr.TerminalNode {
	return s.GetToken(FormulaMINUS, 0)
}

func (s *UnaryOpContext) Expr() IExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *UnaryOpContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitUnaryOp(s)

	default:
		return t.VisitChildren(s)
	}
}

type StringLiteralContext struct {
	ExprContext
}

func NewStringLiteralContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *StringLiteralContext {
	var p = new(StringLiteralContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *StringLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *StringLiteralContext) SINGLEQ_STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(FormulaSINGLEQ_STRING_LITERAL, 0)
}

func (s *StringLiteralContext) DOUBLEQ_STRING_LITERAL() antlr.TerminalNode {
	return s.GetToken(FormulaDOUBLEQ_STRING_LITERAL, 0)
}

func (s *StringLiteralContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitStringLiteral(s)

	default:
		return t.VisitChildren(s)
	}
}

type BracketsContext struct {
	ExprContext
}

func NewBracketsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BracketsContext {
	var p = new(BracketsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *BracketsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BracketsContext) OPEN_PAREN() antlr.TerminalNode {
	return s.GetToken(FormulaOPEN_PAREN, 0)
}

func (s *BracketsContext) Expr() IExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *BracketsContext) CLOSE_PAREN() antlr.TerminalNode {
	return s.GetToken(FormulaCLOSE_PAREN, 0)
}

func (s *BracketsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitBrackets(s)

	default:
		return t.VisitChildren(s)
	}
}

type BooleanLiteralContext struct {
	ExprContext
}

func NewBooleanLiteralContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BooleanLiteralContext {
	var p = new(BooleanLiteralContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *BooleanLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BooleanLiteralContext) TRUE() antlr.TerminalNode {
	return s.GetToken(FormulaTRUE, 0)
}

func (s *BooleanLiteralContext) FALSE() antlr.TerminalNode {
	return s.GetToken(FormulaFALSE, 0)
}

func (s *BooleanLiteralContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitBooleanLiteral(s)

	default:
		return t.VisitChildren(s)
	}
}

type RightWhitespaceOrCommentsContext struct {
	ExprContext
}

func NewRightWhitespaceOrCommentsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *RightWhitespaceOrCommentsContext {
	var p = new(RightWhitespaceOrCommentsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *RightWhitespaceOrCommentsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *RightWhitespaceOrCommentsContext) Expr() IExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *RightWhitespaceOrCommentsContext) Ws_or_comment() IWs_or_commentContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IWs_or_commentContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IWs_or_commentContext)
}

func (s *RightWhitespaceOrCommentsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitRightWhitespaceOrComments(s)

	default:
		return t.VisitChildren(s)
	}
}

type DecimalLiteralContext struct {
	ExprContext
}

func NewDecimalLiteralContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *DecimalLiteralContext {
	var p = new(DecimalLiteralContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *DecimalLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *DecimalLiteralContext) NUMERIC_LITERAL() antlr.TerminalNode {
	return s.GetToken(FormulaNUMERIC_LITERAL, 0)
}

func (s *DecimalLiteralContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitDecimalLiteral(s)

	default:
		return t.VisitChildren(s)
	}
}

type LeftWhitespaceOrCommentsContext struct {
	ExprContext
}

func NewLeftWhitespaceOrCommentsContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *LeftWhitespaceOrCommentsContext {
	var p = new(LeftWhitespaceOrCommentsContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *LeftWhitespaceOrCommentsContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *LeftWhitespaceOrCommentsContext) Ws_or_comment() IWs_or_commentContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IWs_or_commentContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IWs_or_commentContext)
}

func (s *LeftWhitespaceOrCommentsContext) Expr() IExprContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *LeftWhitespaceOrCommentsContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitLeftWhitespaceOrComments(s)

	default:
		return t.VisitChildren(s)
	}
}

type FunctionCallContext struct {
	ExprContext
}

func NewFunctionCallContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *FunctionCallContext {
	var p = new(FunctionCallContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *FunctionCallContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *FunctionCallContext) Func_name() IFunc_nameContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IFunc_nameContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IFunc_nameContext)
}

func (s *FunctionCallContext) OPEN_PAREN() antlr.TerminalNode {
	return s.GetToken(FormulaOPEN_PAREN, 0)
}

func (s *FunctionCallContext) CLOSE_PAREN() antlr.TerminalNode {
	return s.GetToken(FormulaCLOSE_PAREN, 0)
}

func (s *FunctionCallContext) AllExpr() []IExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExprContext); ok {
			len++
		}
	}

	tst := make([]IExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExprContext); ok {
			tst[i] = t.(IExprContext)
			i++
		}
	}

	return tst
}

func (s *FunctionCallContext) Expr(i int) IExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *FunctionCallContext) AllCOMMA() []antlr.TerminalNode {
	return s.GetTokens(FormulaCOMMA)
}

func (s *FunctionCallContext) COMMA(i int) antlr.TerminalNode {
	return s.GetToken(FormulaCOMMA, i)
}

func (s *FunctionCallContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitFunctionCall(s)

	default:
		return t.VisitChildren(s)
	}
}

type IntegerLiteralContext struct {
	ExprContext
}

func NewIntegerLiteralContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *IntegerLiteralContext {
	var p = new(IntegerLiteralContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *IntegerLiteralContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IntegerLiteralContext) INTEGER_LITERAL() antlr.TerminalNode {
	return s.GetToken(FormulaINTEGER_LITERAL, 0)
}

func (s *IntegerLiteralContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitIntegerLiteral(s)

	default:
		return t.VisitChildren(s)
	}
}

type BinaryOpContext struct {
	ExprContext
	op antlr.Token
}

func NewBinaryOpContext(parser antlr.Parser, ctx antlr.ParserRuleContext) *BinaryOpContext {
	var p = new(BinaryOpContext)

	InitEmptyExprContext(&p.ExprContext)
	p.parser = parser
	p.CopyAll(ctx.(*ExprContext))

	return p
}

func (s *BinaryOpContext) GetOp() antlr.Token { return s.op }

func (s *BinaryOpContext) SetOp(v antlr.Token) { s.op = v }

func (s *BinaryOpContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *BinaryOpContext) AllExpr() []IExprContext {
	children := s.GetChildren()
	len := 0
	for _, ctx := range children {
		if _, ok := ctx.(IExprContext); ok {
			len++
		}
	}

	tst := make([]IExprContext, len)
	i := 0
	for _, ctx := range children {
		if t, ok := ctx.(IExprContext); ok {
			tst[i] = t.(IExprContext)
			i++
		}
	}

	return tst
}

func (s *BinaryOpContext) Expr(i int) IExprContext {
	var t antlr.RuleContext
	j := 0
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IExprContext); ok {
			if j == i {
				t = ctx.(antlr.RuleContext)
				break
			}
			j++
		}
	}

	if t == nil {
		return nil
	}

	return t.(IExprContext)
}

func (s *BinaryOpContext) SLASH() antlr.TerminalNode {
	return s.GetToken(FormulaSLASH, 0)
}

func (s *BinaryOpContext) STAR() antlr.TerminalNode {
	return s.GetToken(FormulaSTAR, 0)
}

func (s *BinaryOpContext) PERCENT() antlr.TerminalNode {
	return s.GetToken(FormulaPERCENT, 0)
}

func (s *BinaryOpContext) PLUS() antlr.TerminalNode {
	return s.GetToken(FormulaPLUS, 0)
}

func (s *BinaryOpContext) MINUS() antlr.TerminalNode {
	return s.GetToken(FormulaMINUS, 0)
}

func (s *BinaryOpContext) GT() antlr.TerminalNode {
	return s.GetToken(FormulaGT, 0)
}

func (s *BinaryOpContext) LT() antlr.TerminalNode {
	return s.GetToken(FormulaLT, 0)
}

func (s *BinaryOpContext) GTE() antlr.TerminalNode {
	return s.GetToken(FormulaGTE, 0)
}

func (s *BinaryOpContext) LTE() antlr.TerminalNode {
	return s.GetToken(FormulaLTE, 0)
}

func (s *BinaryOpContext) EQUAL() antlr.TerminalNode {
	return s.GetToken(FormulaEQUAL, 0)
}

func (s *BinaryOpContext) BANG_EQUAL() antlr.TerminalNode {
	return s.GetToken(FormulaBANG_EQUAL, 0)
}

func (s *BinaryOpContext) AMP_AMP() antlr.TerminalNode {
	return s.GetToken(FormulaAMP_AMP, 0)
}

func (s *BinaryOpContext) PIPE_PIPE() antlr.TerminalNode {
	return s.GetToken(FormulaPIPE_PIPE, 0)
}

func (s *BinaryOpContext) AMP() antlr.TerminalNode {
	return s.GetToken(FormulaAMP, 0)
}

func (s *BinaryOpContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitBinaryOp(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *Formula) Expr() (localctx IExprContext) {
	return p.expr(0)
}

func (p *Formula) expr(_p int) (localctx IExprContext) {
	var _parentctx antlr.ParserRuleContext = p.GetParserRuleContext()

	_parentState := p.GetState()
	localctx = NewExprContext(p, p.GetParserRuleContext(), _parentState)
	var _prevctx IExprContext = localctx
	var _ antlr.ParserRuleContext = _prevctx // TODO: To prevent unused variable warning.
	_startState := 2
	p.EnterRecursionRule(localctx, 2, FormulaRULE_expr, _p)
	var _la int

	var _alt int

	p.EnterOuterAlt(localctx, 1)
	p.SetState(47)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}

	switch p.GetTokenStream().LA(1) {
	case FormulaSINGLEQ_STRING_LITERAL:
		localctx = NewStringLiteralContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx

		{
			p.SetState(18)
			p.Match(FormulaSINGLEQ_STRING_LITERAL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case FormulaDOUBLEQ_STRING_LITERAL:
		localctx = NewStringLiteralContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(19)
			p.Match(FormulaDOUBLEQ_STRING_LITERAL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case FormulaINTEGER_LITERAL:
		localctx = NewIntegerLiteralContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(20)
			p.Match(FormulaINTEGER_LITERAL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case FormulaNUMERIC_LITERAL:
		localctx = NewDecimalLiteralContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(21)
			p.Match(FormulaNUMERIC_LITERAL)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case FormulaTRUE, FormulaFALSE:
		localctx = NewBooleanLiteralContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(22)
			_la = p.GetTokenStream().LA(1)

			if !(_la == FormulaTRUE || _la == FormulaFALSE) {
				p.GetErrorHandler().RecoverInline(p)
			} else {
				p.GetErrorHandler().ReportMatch(p)
				p.Consume()
			}
		}

	case FormulaBLOCK_COMMENT, FormulaLINE_COMMENT, FormulaWHITESPACE:
		localctx = NewLeftWhitespaceOrCommentsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(23)
			p.Ws_or_comment()
		}
		{
			p.SetState(24)
			p.expr(13)
		}

	case FormulaOPEN_PAREN:
		localctx = NewBracketsContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(26)
			p.Match(FormulaOPEN_PAREN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(27)
			p.expr(0)
		}
		{
			p.SetState(28)
			p.Match(FormulaCLOSE_PAREN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	case FormulaMINUS:
		localctx = NewUnaryOpContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(30)
			p.Match(FormulaMINUS)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		{
			p.SetState(31)
			p.expr(10)
		}

	case FormulaIDENTIFIER_VARIABLE:
		localctx = NewFieldReferenceCurlyContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(32)
			p.Field_reference_curly()
		}

	case FormulaIDENTIFIER_UNICODE, FormulaIDENTIFIER:
		localctx = NewFunctionCallContext(p, localctx)
		p.SetParserRuleContext(localctx)
		_prevctx = localctx
		{
			p.SetState(33)
			p.Func_name()
		}
		{
			p.SetState(34)
			p.Match(FormulaOPEN_PAREN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}
		p.SetState(43)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_la = p.GetTokenStream().LA(1)

		if (int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&4611686019473875006) != 0 {
			{
				p.SetState(35)
				p.expr(0)
			}
			p.SetState(40)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}
			_la = p.GetTokenStream().LA(1)

			for _la == FormulaCOMMA {
				{
					p.SetState(36)
					p.Match(FormulaCOMMA)
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(37)
					p.expr(0)
				}

				p.SetState(42)
				p.GetErrorHandler().Sync(p)
				if p.HasError() {
					goto errorExit
				}
				_la = p.GetTokenStream().LA(1)
			}

		}
		{
			p.SetState(45)
			p.Match(FormulaCLOSE_PAREN)
			if p.HasError() {
				// Recognition error - abort rule
				goto errorExit
			}
		}

	default:
		p.SetError(antlr.NewNoViableAltException(p, nil, nil, nil, nil, nil))
		goto errorExit
	}
	p.GetParserRuleContext().SetStop(p.GetTokenStream().LT(-1))
	p.SetState(74)
	p.GetErrorHandler().Sync(p)
	if p.HasError() {
		goto errorExit
	}
	_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 4, p.GetParserRuleContext())
	if p.HasError() {
		goto errorExit
	}
	for _alt != 2 && _alt != antlr.ATNInvalidAltNumber {
		if _alt == 1 {
			if p.GetParseListeners() != nil {
				p.TriggerExitRuleEvent()
			}
			_prevctx = localctx
			p.SetState(72)
			p.GetErrorHandler().Sync(p)
			if p.HasError() {
				goto errorExit
			}

			switch p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 3, p.GetParserRuleContext()) {
			case 1:
				localctx = NewBinaryOpContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, FormulaRULE_expr)
				p.SetState(49)

				if !(p.Precpred(p.GetParserRuleContext(), 9)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 9)", ""))
					goto errorExit
				}
				{
					p.SetState(50)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*BinaryOpContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !((int64((_la-12)) & ^0x3f) == 0 && ((int64(1)<<(_la-12))&4613937818241073153) != 0) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*BinaryOpContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(51)
					p.expr(10)
				}

			case 2:
				localctx = NewBinaryOpContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, FormulaRULE_expr)
				p.SetState(52)

				if !(p.Precpred(p.GetParserRuleContext(), 8)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 8)", ""))
					goto errorExit
				}
				{
					p.SetState(53)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*BinaryOpContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == FormulaMINUS || _la == FormulaPLUS) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*BinaryOpContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(54)
					p.expr(9)
				}

			case 3:
				localctx = NewBinaryOpContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, FormulaRULE_expr)
				p.SetState(55)

				if !(p.Precpred(p.GetParserRuleContext(), 7)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 7)", ""))
					goto errorExit
				}
				{
					p.SetState(56)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*BinaryOpContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&27034791903756288) != 0) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*BinaryOpContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(57)
					p.expr(8)
				}

			case 4:
				localctx = NewBinaryOpContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, FormulaRULE_expr)
				p.SetState(58)

				if !(p.Precpred(p.GetParserRuleContext(), 6)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 6)", ""))
					goto errorExit
				}
				{
					p.SetState(59)

					var _lt = p.GetTokenStream().LT(1)

					localctx.(*BinaryOpContext).op = _lt

					_la = p.GetTokenStream().LA(1)

					if !(_la == FormulaBANG_EQUAL || _la == FormulaEQUAL) {
						var _ri = p.GetErrorHandler().RecoverInline(p)

						localctx.(*BinaryOpContext).op = _ri
					} else {
						p.GetErrorHandler().ReportMatch(p)
						p.Consume()
					}
				}
				{
					p.SetState(60)
					p.expr(7)
				}

			case 5:
				localctx = NewBinaryOpContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, FormulaRULE_expr)
				p.SetState(61)

				if !(p.Precpred(p.GetParserRuleContext(), 5)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 5)", ""))
					goto errorExit
				}
				{
					p.SetState(62)

					var _m = p.Match(FormulaAMP_AMP)

					localctx.(*BinaryOpContext).op = _m
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(63)
					p.expr(6)
				}

			case 6:
				localctx = NewBinaryOpContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, FormulaRULE_expr)
				p.SetState(64)

				if !(p.Precpred(p.GetParserRuleContext(), 4)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 4)", ""))
					goto errorExit
				}
				{
					p.SetState(65)

					var _m = p.Match(FormulaPIPE_PIPE)

					localctx.(*BinaryOpContext).op = _m
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(66)
					p.expr(5)
				}

			case 7:
				localctx = NewBinaryOpContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, FormulaRULE_expr)
				p.SetState(67)

				if !(p.Precpred(p.GetParserRuleContext(), 3)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 3)", ""))
					goto errorExit
				}
				{
					p.SetState(68)

					var _m = p.Match(FormulaAMP)

					localctx.(*BinaryOpContext).op = _m
					if p.HasError() {
						// Recognition error - abort rule
						goto errorExit
					}
				}
				{
					p.SetState(69)
					p.expr(4)
				}

			case 8:
				localctx = NewRightWhitespaceOrCommentsContext(p, NewExprContext(p, _parentctx, _parentState))
				p.PushNewRecursionContext(localctx, _startState, FormulaRULE_expr)
				p.SetState(70)

				if !(p.Precpred(p.GetParserRuleContext(), 12)) {
					p.SetError(antlr.NewFailedPredicateException(p, "p.Precpred(p.GetParserRuleContext(), 12)", ""))
					goto errorExit
				}
				{
					p.SetState(71)
					p.Ws_or_comment()
				}

			case antlr.ATNInvalidAltNumber:
				goto errorExit
			}

		}
		p.SetState(76)
		p.GetErrorHandler().Sync(p)
		if p.HasError() {
			goto errorExit
		}
		_alt = p.GetInterpreter().AdaptivePredict(p.BaseParser, p.GetTokenStream(), 4, p.GetParserRuleContext())
		if p.HasError() {
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.UnrollRecursionContexts(_parentctx)
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IWs_or_commentContext is an interface to support dynamic dispatch.
type IWs_or_commentContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	BLOCK_COMMENT() antlr.TerminalNode
	LINE_COMMENT() antlr.TerminalNode
	WHITESPACE() antlr.TerminalNode

	// IsWs_or_commentContext differentiates from other interfaces.
	IsWs_or_commentContext()
}

type Ws_or_commentContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyWs_or_commentContext() *Ws_or_commentContext {
	var p = new(Ws_or_commentContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_ws_or_comment
	return p
}

func InitEmptyWs_or_commentContext(p *Ws_or_commentContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_ws_or_comment
}

func (*Ws_or_commentContext) IsWs_or_commentContext() {}

func NewWs_or_commentContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Ws_or_commentContext {
	var p = new(Ws_or_commentContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = FormulaRULE_ws_or_comment

	return p
}

func (s *Ws_or_commentContext) GetParser() antlr.Parser { return s.parser }

func (s *Ws_or_commentContext) BLOCK_COMMENT() antlr.TerminalNode {
	return s.GetToken(FormulaBLOCK_COMMENT, 0)
}

func (s *Ws_or_commentContext) LINE_COMMENT() antlr.TerminalNode {
	return s.GetToken(FormulaLINE_COMMENT, 0)
}

func (s *Ws_or_commentContext) WHITESPACE() antlr.TerminalNode {
	return s.GetToken(FormulaWHITESPACE, 0)
}

func (s *Ws_or_commentContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Ws_or_commentContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Ws_or_commentContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitWs_or_comment(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *Formula) Ws_or_comment() (localctx IWs_or_commentContext) {
	localctx = NewWs_or_commentContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 4, FormulaRULE_ws_or_comment)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(77)
		_la = p.GetTokenStream().LA(1)

		if !((int64(_la) & ^0x3f) == 0 && ((int64(1)<<_la)&14) != 0) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IField_referenceContext is an interface to support dynamic dispatch.
type IField_referenceContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	IDENTIFIER_UNICODE() antlr.TerminalNode

	// IsField_referenceContext differentiates from other interfaces.
	IsField_referenceContext()
}

type Field_referenceContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyField_referenceContext() *Field_referenceContext {
	var p = new(Field_referenceContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_field_reference
	return p
}

func InitEmptyField_referenceContext(p *Field_referenceContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_field_reference
}

func (*Field_referenceContext) IsField_referenceContext() {}

func NewField_referenceContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Field_referenceContext {
	var p = new(Field_referenceContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = FormulaRULE_field_reference

	return p
}

func (s *Field_referenceContext) GetParser() antlr.Parser { return s.parser }

func (s *Field_referenceContext) IDENTIFIER_UNICODE() antlr.TerminalNode {
	return s.GetToken(FormulaIDENTIFIER_UNICODE, 0)
}

func (s *Field_referenceContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Field_referenceContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Field_referenceContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitField_reference(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *Formula) Field_reference() (localctx IField_referenceContext) {
	localctx = NewField_referenceContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 6, FormulaRULE_field_reference)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(79)
		p.Match(FormulaIDENTIFIER_UNICODE)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IField_reference_curlyContext is an interface to support dynamic dispatch.
type IField_reference_curlyContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	IDENTIFIER_VARIABLE() antlr.TerminalNode

	// IsField_reference_curlyContext differentiates from other interfaces.
	IsField_reference_curlyContext()
}

type Field_reference_curlyContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyField_reference_curlyContext() *Field_reference_curlyContext {
	var p = new(Field_reference_curlyContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_field_reference_curly
	return p
}

func InitEmptyField_reference_curlyContext(p *Field_reference_curlyContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_field_reference_curly
}

func (*Field_reference_curlyContext) IsField_reference_curlyContext() {}

func NewField_reference_curlyContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Field_reference_curlyContext {
	var p = new(Field_reference_curlyContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = FormulaRULE_field_reference_curly

	return p
}

func (s *Field_reference_curlyContext) GetParser() antlr.Parser { return s.parser }

func (s *Field_reference_curlyContext) IDENTIFIER_VARIABLE() antlr.TerminalNode {
	return s.GetToken(FormulaIDENTIFIER_VARIABLE, 0)
}

func (s *Field_reference_curlyContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Field_reference_curlyContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Field_reference_curlyContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitField_reference_curly(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *Formula) Field_reference_curly() (localctx IField_reference_curlyContext) {
	localctx = NewField_reference_curlyContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 8, FormulaRULE_field_reference_curly)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(81)
		p.Match(FormulaIDENTIFIER_VARIABLE)
		if p.HasError() {
			// Recognition error - abort rule
			goto errorExit
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IFunc_nameContext is an interface to support dynamic dispatch.
type IFunc_nameContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	Identifier() IIdentifierContext

	// IsFunc_nameContext differentiates from other interfaces.
	IsFunc_nameContext()
}

type Func_nameContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyFunc_nameContext() *Func_nameContext {
	var p = new(Func_nameContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_func_name
	return p
}

func InitEmptyFunc_nameContext(p *Func_nameContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_func_name
}

func (*Func_nameContext) IsFunc_nameContext() {}

func NewFunc_nameContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *Func_nameContext {
	var p = new(Func_nameContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = FormulaRULE_func_name

	return p
}

func (s *Func_nameContext) GetParser() antlr.Parser { return s.parser }

func (s *Func_nameContext) Identifier() IIdentifierContext {
	var t antlr.RuleContext
	for _, ctx := range s.GetChildren() {
		if _, ok := ctx.(IIdentifierContext); ok {
			t = ctx.(antlr.RuleContext)
			break
		}
	}

	if t == nil {
		return nil
	}

	return t.(IIdentifierContext)
}

func (s *Func_nameContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *Func_nameContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *Func_nameContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitFunc_name(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *Formula) Func_name() (localctx IFunc_nameContext) {
	localctx = NewFunc_nameContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 10, FormulaRULE_func_name)
	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(83)
		p.Identifier()
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

// IIdentifierContext is an interface to support dynamic dispatch.
type IIdentifierContext interface {
	antlr.ParserRuleContext

	// GetParser returns the parser.
	GetParser() antlr.Parser

	// Getter signatures
	IDENTIFIER() antlr.TerminalNode
	IDENTIFIER_UNICODE() antlr.TerminalNode

	// IsIdentifierContext differentiates from other interfaces.
	IsIdentifierContext()
}

type IdentifierContext struct {
	antlr.BaseParserRuleContext
	parser antlr.Parser
}

func NewEmptyIdentifierContext() *IdentifierContext {
	var p = new(IdentifierContext)
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_identifier
	return p
}

func InitEmptyIdentifierContext(p *IdentifierContext) {
	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, nil, -1)
	p.RuleIndex = FormulaRULE_identifier
}

func (*IdentifierContext) IsIdentifierContext() {}

func NewIdentifierContext(parser antlr.Parser, parent antlr.ParserRuleContext, invokingState int) *IdentifierContext {
	var p = new(IdentifierContext)

	antlr.InitBaseParserRuleContext(&p.BaseParserRuleContext, parent, invokingState)

	p.parser = parser
	p.RuleIndex = FormulaRULE_identifier

	return p
}

func (s *IdentifierContext) GetParser() antlr.Parser { return s.parser }

func (s *IdentifierContext) IDENTIFIER() antlr.TerminalNode {
	return s.GetToken(FormulaIDENTIFIER, 0)
}

func (s *IdentifierContext) IDENTIFIER_UNICODE() antlr.TerminalNode {
	return s.GetToken(FormulaIDENTIFIER_UNICODE, 0)
}

func (s *IdentifierContext) GetRuleContext() antlr.RuleContext {
	return s
}

func (s *IdentifierContext) ToStringTree(ruleNames []string, recog antlr.Recognizer) string {
	return antlr.TreesStringTree(s, ruleNames, recog)
}

func (s *IdentifierContext) Accept(visitor antlr.ParseTreeVisitor) interface{} {
	switch t := visitor.(type) {
	case FormulaVisitor:
		return t.VisitIdentifier(s)

	default:
		return t.VisitChildren(s)
	}
}

func (p *Formula) Identifier() (localctx IIdentifierContext) {
	localctx = NewIdentifierContext(p, p.GetParserRuleContext(), p.GetState())
	p.EnterRule(localctx, 12, FormulaRULE_identifier)
	var _la int

	p.EnterOuterAlt(localctx, 1)
	{
		p.SetState(85)
		_la = p.GetTokenStream().LA(1)

		if !(_la == FormulaIDENTIFIER_UNICODE || _la == FormulaIDENTIFIER) {
			p.GetErrorHandler().RecoverInline(p)
		} else {
			p.GetErrorHandler().ReportMatch(p)
			p.Consume()
		}
	}

errorExit:
	if p.HasError() {
		v := p.GetError()
		localctx.SetException(v)
		p.GetErrorHandler().ReportError(p, v)
		p.GetErrorHandler().Recover(p, v)
		p.SetError(nil)
	}
	p.ExitRule()
	return localctx
	goto errorExit // Trick to prevent compiler error if the label is not used
}

func (p *Formula) Sempred(localctx antlr.RuleContext, ruleIndex, predIndex int) bool {
	switch ruleIndex {
	case 1:
		var t *ExprContext = nil
		if localctx != nil {
			t = localctx.(*ExprContext)
		}
		return p.Expr_Sempred(t, predIndex)

	default:
		panic("No predicate with index: " + fmt.Sprint(ruleIndex))
	}
}

func (p *Formula) Expr_Sempred(localctx antlr.RuleContext, predIndex int) bool {
	switch predIndex {
	case 0:
		return p.Precpred(p.GetParserRuleContext(), 9)

	case 1:
		return p.Precpred(p.GetParserRuleContext(), 8)

	case 2:
		return p.Precpred(p.GetParserRuleContext(), 7)

	case 3:
		return p.Precpred(p.GetParserRuleContext(), 6)

	case 4:
		return p.Precpred(p.GetParserRuleContext(), 5)

	case 5:
		return p.Precpred(p.GetParserRuleContext(), 4)

	case 6:
		return p.Precpred(p.GetParserRuleContext(), 3)

	case 7:
		return p.Precpred(p.GetParserRuleContext(), 12)

	default:
		panic("No predicate with index: " + fmt.Sprint(predIndex))
	}
}
