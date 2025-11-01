export enum LogLevel {
  INFO = 'info',
  SUCCESS = 'success',
  WARNING = 'warning',
  ERROR = 'error'
}

export class Logger {
  private prefix: string

  constructor(prefix: string = 'Demo') {
    this.prefix = prefix
  }

  private formatMessage(level: LogLevel, message: string): string {
    const timestamp = new Date().toISOString()
    const colors = {
      [LogLevel.INFO]: '\x1b[36m',    // Cyan
      [LogLevel.SUCCESS]: '\x1b[32m', // Green
      [LogLevel.WARNING]: '\x1b[33m', // Yellow
      [LogLevel.ERROR]: '\x1b[31m',   // Red
    }
    const reset = '\x1b[0m'
    const color = colors[level] || ''
    
    return `${color}[${timestamp}] [${this.prefix}] [${level.toUpperCase()}]${reset} ${message}`
  }

  info(message: string): void {
    console.log(this.formatMessage(LogLevel.INFO, message))
  }

  success(message: string): void {
    console.log(this.formatMessage(LogLevel.SUCCESS, message))
  }

  warning(message: string): void {
    console.warn(this.formatMessage(LogLevel.WARNING, message))
  }

  error(message: string): void {
    console.error(this.formatMessage(LogLevel.ERROR, message))
  }

  section(title: string): void {
    console.log('\n' + '='.repeat(60))
    console.log(`  ${title}`)
    console.log('='.repeat(60) + '\n')
  }

  step(step: number, total: number, message: string): void {
    console.log(`[${step}/${total}] ${message}`)
  }
}


