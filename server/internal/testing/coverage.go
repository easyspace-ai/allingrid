package testing

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// CoverageAnalyzer 覆盖率分析器
type CoverageAnalyzer struct {
	logger     *zap.Logger
	projectDir string
	outputDir  string
}

// NewCoverageAnalyzer 创建覆盖率分析器
func NewCoverageAnalyzer(logger *zap.Logger, projectDir, outputDir string) *CoverageAnalyzer {
	return &CoverageAnalyzer{
		logger:     logger,
		projectDir: projectDir,
		outputDir:  outputDir,
	}
}

// CoverageReport 覆盖率报告
type CoverageReport struct {
	Timestamp       time.Time                   `json:"timestamp"`
	OverallCoverage float64                     `json:"overall_coverage"`
	PackageCoverage map[string]*PackageCoverage `json:"package_coverage"`
	FileCoverage    map[string]*FileCoverage    `json:"file_coverage"`
	Summary         *CoverageSummary            `json:"summary"`
	Thresholds      *CoverageThresholds         `json:"thresholds"`
	Status          CoverageStatus              `json:"status"`
}

// PackageCoverage 包覆盖率
type PackageCoverage struct {
	Package    string   `json:"package"`
	Coverage   float64  `json:"coverage"`
	Statements int      `json:"statements"`
	Covered    int      `json:"covered"`
	Files      []string `json:"files"`
}

// FileCoverage 文件覆盖率
type FileCoverage struct {
	File       string          `json:"file"`
	Package    string          `json:"package"`
	Coverage   float64         `json:"coverage"`
	Statements int             `json:"statements"`
	Covered    int             `json:"covered"`
	Lines      []*LineCoverage `json:"lines"`
}

// LineCoverage 行覆盖率
type LineCoverage struct {
	LineNumber int  `json:"line_number"`
	Covered    bool `json:"covered"`
	Count      int  `json:"count"`
}

// CoverageSummary 覆盖率摘要
type CoverageSummary struct {
	TotalPackages     int     `json:"total_packages"`
	CoveredPackages   int     `json:"covered_packages"`
	TotalFiles        int     `json:"total_files"`
	CoveredFiles      int     `json:"covered_files"`
	TotalStatements   int     `json:"total_statements"`
	CoveredStatements int     `json:"covered_statements"`
	AverageCoverage   float64 `json:"average_coverage"`
}

// CoverageThresholds 覆盖率阈值
type CoverageThresholds struct {
	Overall float64 `json:"overall"`
	Package float64 `json:"package"`
	File    float64 `json:"file"`
}

// CoverageStatus 覆盖率状态
type CoverageStatus string

const (
	CoverageStatusPassed  CoverageStatus = "passed"
	CoverageStatusFailed  CoverageStatus = "failed"
	CoverageStatusWarning CoverageStatus = "warning"
)

// GenerateReport 生成覆盖率报告
func (ca *CoverageAnalyzer) GenerateReport(testPackages []string, thresholds *CoverageThresholds) (*CoverageReport, error) {
	ca.logger.Info("Generating coverage report", zap.Strings("packages", testPackages))

	// 运行测试并生成覆盖率数据
	coverageFile, err := ca.runTestsWithCoverage(testPackages)
	if err != nil {
		return nil, fmt.Errorf("failed to run tests with coverage: %w", err)
	}
	defer os.Remove(coverageFile)

	// 解析覆盖率数据
	report, err := ca.parseCoverageData(coverageFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse coverage data: %w", err)
	}

	// 设置阈值和状态
	report.Thresholds = thresholds
	report.Status = ca.evaluateCoverageStatus(report, thresholds)
	report.Timestamp = time.Now()

	// 保存报告
	if err := ca.saveReport(report); err != nil {
		ca.logger.Warn("Failed to save coverage report", zap.Error(err))
	}

	// 生成HTML报告
	if err := ca.generateHTMLReport(report); err != nil {
		ca.logger.Warn("Failed to generate HTML report", zap.Error(err))
	}

	ca.logger.Info("Coverage report generated successfully",
		zap.Float64("overall_coverage", report.OverallCoverage),
		zap.String("status", string(report.Status)))

	return report, nil
}

// runTestsWithCoverage 运行测试并生成覆盖率数据
func (ca *CoverageAnalyzer) runTestsWithCoverage(packages []string) (string, error) {
	coverageFile := filepath.Join(ca.outputDir, "coverage.out")

	// 构建测试命令
	args := []string{"test", "-coverprofile=" + coverageFile, "-covermode=atomic"}
	args = append(args, packages...)

	cmd := exec.Command("go", args...)
	cmd.Dir = ca.projectDir

	output, err := cmd.CombinedOutput()
	if err != nil {
		ca.logger.Error("Test execution failed",
			zap.Error(err),
			zap.String("output", string(output)))
		return "", fmt.Errorf("test execution failed: %w", err)
	}

	return coverageFile, nil
}

// parseCoverageData 解析覆盖率数据
func (ca *CoverageAnalyzer) parseCoverageData(coverageFile string) (*CoverageReport, error) {
	file, err := os.Open(coverageFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open coverage file: %w", err)
	}
	defer file.Close()

	report := &CoverageReport{
		PackageCoverage: make(map[string]*PackageCoverage),
		FileCoverage:    make(map[string]*FileCoverage),
		Summary:         &CoverageSummary{},
	}

	scanner := bufio.NewScanner(file)

	// 跳过第一行（mode行）
	if scanner.Scan() {
		// mode: atomic
	}

	// 解析覆盖率数据
	for scanner.Scan() {
		line := scanner.Text()
		if err := ca.parseCoverageLine(line, report); err != nil {
			ca.logger.Warn("Failed to parse coverage line",
				zap.String("line", line),
				zap.Error(err))
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan coverage file: %w", err)
	}

	// 计算汇总信息
	ca.calculateSummary(report)

	return report, nil
}

// parseCoverageLine 解析覆盖率行
func (ca *CoverageAnalyzer) parseCoverageLine(line string, report *CoverageReport) error {
	// 格式: file:startLine.startCol,endLine.endCol numStmt count
	re := regexp.MustCompile(`^(.+):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)$`)
	matches := re.FindStringSubmatch(line)
	if len(matches) != 8 {
		return fmt.Errorf("invalid coverage line format: %s", line)
	}

	fileName := matches[1]
	startLine, _ := strconv.Atoi(matches[2])
	endLine, _ := strconv.Atoi(matches[4])
	numStmt, _ := strconv.Atoi(matches[6])
	count, _ := strconv.Atoi(matches[7])

	// 获取包名
	packageName := ca.getPackageFromFile(fileName)

	// 更新文件覆盖率
	if _, exists := report.FileCoverage[fileName]; !exists {
		report.FileCoverage[fileName] = &FileCoverage{
			File:    fileName,
			Package: packageName,
			Lines:   make([]*LineCoverage, 0),
		}
	}

	fileCov := report.FileCoverage[fileName]
	fileCov.Statements += numStmt
	if count > 0 {
		fileCov.Covered += numStmt
	}

	// 添加行覆盖率信息
	for lineNum := startLine; lineNum <= endLine; lineNum++ {
		fileCov.Lines = append(fileCov.Lines, &LineCoverage{
			LineNumber: lineNum,
			Covered:    count > 0,
			Count:      count,
		})
	}

	// 更新包覆盖率
	if _, exists := report.PackageCoverage[packageName]; !exists {
		report.PackageCoverage[packageName] = &PackageCoverage{
			Package: packageName,
			Files:   make([]string, 0),
		}
	}

	pkgCov := report.PackageCoverage[packageName]
	pkgCov.Statements += numStmt
	if count > 0 {
		pkgCov.Covered += numStmt
	}

	// 添加文件到包中
	if !contains(pkgCov.Files, fileName) {
		pkgCov.Files = append(pkgCov.Files, fileName)
	}

	return nil
}

// getPackageFromFile 从文件路径获取包名
func (ca *CoverageAnalyzer) getPackageFromFile(fileName string) string {
	// 移除项目根目录前缀
	relPath := strings.TrimPrefix(fileName, ca.projectDir+"/")

	// 获取目录路径作为包名
	dir := filepath.Dir(relPath)
	if dir == "." {
		return "main"
	}

	return dir
}

// calculateSummary 计算汇总信息
func (ca *CoverageAnalyzer) calculateSummary(report *CoverageReport) {
	summary := report.Summary

	// 计算包级别覆盖率
	for _, pkgCov := range report.PackageCoverage {
		if pkgCov.Statements > 0 {
			pkgCov.Coverage = float64(pkgCov.Covered) / float64(pkgCov.Statements) * 100
		}

		summary.TotalPackages++
		if pkgCov.Coverage > 0 {
			summary.CoveredPackages++
		}
		summary.TotalStatements += pkgCov.Statements
		summary.CoveredStatements += pkgCov.Covered
	}

	// 计算文件级别覆盖率
	for _, fileCov := range report.FileCoverage {
		if fileCov.Statements > 0 {
			fileCov.Coverage = float64(fileCov.Covered) / float64(fileCov.Statements) * 100
		}

		summary.TotalFiles++
		if fileCov.Coverage > 0 {
			summary.CoveredFiles++
		}
	}

	// 计算总体覆盖率
	if summary.TotalStatements > 0 {
		report.OverallCoverage = float64(summary.CoveredStatements) / float64(summary.TotalStatements) * 100
	}

	// 计算平均覆盖率
	if summary.TotalPackages > 0 {
		var totalCoverage float64
		for _, pkgCov := range report.PackageCoverage {
			totalCoverage += pkgCov.Coverage
		}
		summary.AverageCoverage = totalCoverage / float64(summary.TotalPackages)
	}
}

// evaluateCoverageStatus 评估覆盖率状态
func (ca *CoverageAnalyzer) evaluateCoverageStatus(report *CoverageReport, thresholds *CoverageThresholds) CoverageStatus {
	if thresholds == nil {
		return CoverageStatusPassed
	}

	// 检查总体覆盖率
	if report.OverallCoverage < thresholds.Overall {
		return CoverageStatusFailed
	}

	// 检查包覆盖率
	for _, pkgCov := range report.PackageCoverage {
		if pkgCov.Coverage < thresholds.Package {
			return CoverageStatusWarning
		}
	}

	// 检查文件覆盖率
	for _, fileCov := range report.FileCoverage {
		if fileCov.Coverage < thresholds.File {
			return CoverageStatusWarning
		}
	}

	return CoverageStatusPassed
}

// saveReport 保存报告
func (ca *CoverageAnalyzer) saveReport(report *CoverageReport) error {
	reportFile := filepath.Join(ca.outputDir, "coverage-report.json")

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	if err := ioutil.WriteFile(reportFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write report file: %w", err)
	}

	ca.logger.Info("Coverage report saved", zap.String("file", reportFile))
	return nil
}

// generateHTMLReport 生成HTML报告
func (ca *CoverageAnalyzer) generateHTMLReport(report *CoverageReport) error {
	htmlFile := filepath.Join(ca.outputDir, "coverage-report.html")

	html := ca.buildHTMLReport(report)

	if err := ioutil.WriteFile(htmlFile, []byte(html), 0644); err != nil {
		return fmt.Errorf("failed to write HTML report: %w", err)
	}

	ca.logger.Info("HTML coverage report generated", zap.String("file", htmlFile))
	return nil
}

// buildHTMLReport 构建HTML报告
func (ca *CoverageAnalyzer) buildHTMLReport(report *CoverageReport) string {
	var html strings.Builder

	html.WriteString(`<!DOCTYPE html>
<html>
<head>
    <title>Coverage Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #f5f5f5; padding: 20px; border-radius: 5px; }
        .summary { margin: 20px 0; }
        .package { margin: 10px 0; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }
        .coverage-bar { width: 200px; height: 20px; background-color: #f0f0f0; border-radius: 10px; overflow: hidden; }
        .coverage-fill { height: 100%; background-color: #4CAF50; }
        .low-coverage { background-color: #f44336; }
        .medium-coverage { background-color: #ff9800; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th, td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>`)

	// 头部信息
	html.WriteString(fmt.Sprintf(`
    <div class="header">
        <h1>Coverage Report</h1>
        <p>Generated: %s</p>
        <p>Overall Coverage: %.2f%%</p>
        <p>Status: %s</p>
    </div>`,
		report.Timestamp.Format("2006-01-02 15:04:05"),
		report.OverallCoverage,
		report.Status))

	// 摘要信息
	html.WriteString(`
    <div class="summary">
        <h2>Summary</h2>
        <table>
            <tr><th>Metric</th><th>Value</th></tr>`)

	html.WriteString(fmt.Sprintf(`
            <tr><td>Total Packages</td><td>%d</td></tr>
            <tr><td>Covered Packages</td><td>%d</td></tr>
            <tr><td>Total Files</td><td>%d</td></tr>
            <tr><td>Covered Files</td><td>%d</td></tr>
            <tr><td>Total Statements</td><td>%d</td></tr>
            <tr><td>Covered Statements</td><td>%d</td></tr>
            <tr><td>Average Coverage</td><td>%.2f%%</td></tr>`,
		report.Summary.TotalPackages,
		report.Summary.CoveredPackages,
		report.Summary.TotalFiles,
		report.Summary.CoveredFiles,
		report.Summary.TotalStatements,
		report.Summary.CoveredStatements,
		report.Summary.AverageCoverage))

	html.WriteString(`
        </table>
    </div>`)

	// 包覆盖率
	html.WriteString(`
    <div class="packages">
        <h2>Package Coverage</h2>`)

	// 按覆盖率排序包
	packages := make([]*PackageCoverage, 0, len(report.PackageCoverage))
	for _, pkg := range report.PackageCoverage {
		packages = append(packages, pkg)
	}
	sort.Slice(packages, func(i, j int) bool {
		return packages[i].Coverage > packages[j].Coverage
	})

	for _, pkg := range packages {
		coverageClass := "coverage-fill"
		if pkg.Coverage < 50 {
			coverageClass += " low-coverage"
		} else if pkg.Coverage < 80 {
			coverageClass += " medium-coverage"
		}

		html.WriteString(fmt.Sprintf(`
        <div class="package">
            <h3>%s (%.2f%%)</h3>
            <div class="coverage-bar">
                <div class="%s" style="width: %.2f%%"></div>
            </div>
            <p>Statements: %d/%d</p>
            <p>Files: %d</p>
        </div>`,
			pkg.Package,
			pkg.Coverage,
			coverageClass,
			pkg.Coverage,
			pkg.Covered,
			pkg.Statements,
			len(pkg.Files)))
	}

	html.WriteString(`
    </div>`)

	html.WriteString(`
</body>
</html>`)

	return html.String()
}

// contains 检查切片是否包含元素
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// GetDefaultThresholds 获取默认阈值
func GetDefaultThresholds() *CoverageThresholds {
	return &CoverageThresholds{
		Overall: 80.0,
		Package: 70.0,
		File:    60.0,
	}
}

// CoverageReporter 覆盖率报告器
type CoverageReporter struct {
	analyzer *CoverageAnalyzer
	logger   *zap.Logger
}

// NewCoverageReporter 创建覆盖率报告器
func NewCoverageReporter(logger *zap.Logger, projectDir, outputDir string) *CoverageReporter {
	return &CoverageReporter{
		analyzer: NewCoverageAnalyzer(logger, projectDir, outputDir),
		logger:   logger,
	}
}

// GenerateAndReport 生成并报告覆盖率
func (cr *CoverageReporter) GenerateAndReport(packages []string, thresholds *CoverageThresholds) error {
	report, err := cr.analyzer.GenerateReport(packages, thresholds)
	if err != nil {
		return fmt.Errorf("failed to generate coverage report: %w", err)
	}

	// 记录覆盖率信息
	cr.logCoverageReport(report)

	// 如果覆盖率不达标，返回错误
	if report.Status == CoverageStatusFailed {
		return fmt.Errorf("coverage below threshold: %.2f%% < %.2f%%",
			report.OverallCoverage, thresholds.Overall)
	}

	return nil
}

// logCoverageReport 记录覆盖率报告
func (cr *CoverageReporter) logCoverageReport(report *CoverageReport) {
	cr.logger.Info("Coverage Report Summary",
		zap.Float64("overall_coverage", report.OverallCoverage),
		zap.String("status", string(report.Status)),
		zap.Int("total_packages", report.Summary.TotalPackages),
		zap.Int("total_files", report.Summary.TotalFiles),
		zap.Int("total_statements", report.Summary.TotalStatements),
		zap.Int("covered_statements", report.Summary.CoveredStatements))

	// 记录低覆盖率的包
	for _, pkg := range report.PackageCoverage {
		if pkg.Coverage < report.Thresholds.Package {
			cr.logger.Warn("Low package coverage",
				zap.String("package", pkg.Package),
				zap.Float64("coverage", pkg.Coverage),
				zap.Float64("threshold", report.Thresholds.Package))
		}
	}
}
