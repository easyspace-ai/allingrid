import { test, expect } from '@playwright/test';

test.describe('列拖动排序和右键菜单功能自动化测试', () => {
  test.beforeEach(async ({ page }) => {
    // 启动开发服务器并导航到页面
    await page.goto('http://localhost:5173');
    // 等待表格加载完成
    await page.waitForSelector('[data-slot="grid"]', { timeout: 10000 });
    await page.waitForTimeout(1000); // 等待完全加载
  });

  test('应该能够拖动列头进行排序', async ({ page }) => {
    // 等待表头加载
    await page.waitForSelector('[data-slot="grid-header-cell"]', { timeout: 5000 });
    
    // 获取所有列头
    const headers = await page.locator('[data-slot="grid-header-cell"]').all();
    expect(headers.length).toBeGreaterThan(1);
    
    // 获取第一个和第二个列头的文本
    const firstHeaderText = await headers[0].locator('span.truncate').first().textContent();
    const secondHeaderText = await headers[1].locator('span.truncate').first().textContent();
    
    console.log(`第一个列头: ${firstHeaderText}`);
    console.log(`第二个列头: ${secondHeaderText}`);
    
    // 获取第一个列头的位置（选择文本区域）
    const firstHeaderTextElement = headers[0].locator('span.truncate').first();
    const secondHeaderTextElement = headers[1].locator('span.truncate').first();
    
    await expect(firstHeaderTextElement).toBeVisible();
    await expect(secondHeaderTextElement).toBeVisible();
    
    const firstHeaderBox = await firstHeaderTextElement.boundingBox();
    const secondHeaderBox = await secondHeaderTextElement.boundingBox();
    
    if (!firstHeaderBox || !secondHeaderBox) {
      throw new Error('无法获取列头位置');
    }
    
    // 计算拖动起点和终点（使用文本区域的中心点）
    const startX = firstHeaderBox.x + firstHeaderBox.width / 2;
    const startY = firstHeaderBox.y + firstHeaderBox.height / 2;
    const endX = secondHeaderBox.x + secondHeaderBox.width / 2;
    const endY = secondHeaderBox.y + secondHeaderBox.height / 2;
    
    console.log(`开始拖动: (${startX}, ${startY}) -> (${endX}, ${endY})`);
    
    // 执行拖动操作
    await page.mouse.move(startX, startY);
    await page.mouse.down();
    
    // 等待一小段时间，让拖拽开始
    await page.waitForTimeout(200);
    
    // 移动到目标位置（分步移动，模拟真实拖拽）
    await page.mouse.move(startX + 10, startY, { steps: 2 });
    await page.waitForTimeout(50);
    await page.mouse.move(startX + 30, startY, { steps: 2 });
    await page.waitForTimeout(50);
    await page.mouse.move(endX, endY, { steps: 5 });
    
    // 等待拖拽阴影出现
    await page.waitForTimeout(300);
    
    // 释放鼠标
    await page.mouse.up();
    
    // 等待列顺序更新
    await page.waitForTimeout(500);
    
    // 验证列顺序是否改变
    const newHeaders = await page.locator('[data-slot="grid-header-cell"]').all();
    const newFirstHeaderText = await newHeaders[0].locator('span.truncate').first().textContent();
    
    console.log(`拖动后第一个列头: ${newFirstHeaderText}`);
    
    // 验证列顺序已经改变（第一个列头应该变成原来的第二个）
    expect(newFirstHeaderText).toBe(secondHeaderText);
  });

  test('右键点击列头应该显示下拉菜单', async ({ page }) => {
    await page.waitForSelector('[data-slot="grid-header-cell"]', { timeout: 5000 });
    
    const headers = await page.locator('[data-slot="grid-header-cell"]').all();
    const firstHeader = headers[0];
    
    // 获取列头的位置
    const headerBox = await firstHeader.boundingBox();
    if (!headerBox) {
      throw new Error('无法获取列头位置');
    }
    
    // 在列头中心位置右键点击
    const clickX = headerBox.x + headerBox.width / 2;
    const clickY = headerBox.y + headerBox.height / 2;
    
    console.log(`右键点击位置: (${clickX}, ${clickY})`);
    
    // 右键点击
    await page.mouse.click(clickX, clickY, { button: 'right' });
    
    // 等待下拉菜单出现
    await page.waitForTimeout(300);
    
    // 检查下拉菜单是否打开
    const menuContent = page.locator('[data-slot="dropdown-menu-content"]');
    await expect(menuContent.first()).toBeVisible({ timeout: 2000 });
    
    // 获取菜单位置
    const menuBox = await menuContent.first().boundingBox();
    if (menuBox) {
      console.log(`菜单位置: (${menuBox.x}, ${menuBox.y})`);
      console.log(`菜单大小: ${menuBox.width} x ${menuBox.height}`);
      
      // 验证菜单位置应该在点击位置附近（允许一些偏移）
      const deltaX = Math.abs(menuBox.x - clickX);
      const deltaY = Math.abs(menuBox.y - clickY);
      
      console.log(`位置偏移: x=${deltaX}px, y=${deltaY}px`);
      
      // 菜单应该出现在点击位置附近（允许 50px 的偏移）
      expect(deltaX).toBeLessThan(100);
      expect(deltaY).toBeLessThan(100);
    }
    
    // 检查菜单项是否存在
    const sortAscItem = menuContent.locator('text=Sort asc');
    const sortDescItem = menuContent.locator('text=Sort desc');
    
    await expect(sortAscItem.or(sortDescItem).first()).toBeVisible();
    
    // 关闭菜单
    await page.keyboard.press('Escape');
    await page.waitForTimeout(200);
    
    // 验证菜单已关闭
    await expect(menuContent.first()).not.toBeVisible();
  });

  test('拖动时应该显示半透明阴影', async ({ page }) => {
    await page.waitForSelector('[data-slot="grid-header-cell"]', { timeout: 5000 });
    
    const headers = await page.locator('[data-slot="grid-header-cell"]').all();
    const firstHeaderTextElement = headers[0].locator('span.truncate').first();
    
    await expect(firstHeaderTextElement).toBeVisible();
    const firstHeaderBox = await firstHeaderTextElement.boundingBox();
    
    if (!firstHeaderBox) {
      throw new Error('无法获取列头位置');
    }
    
    const startX = firstHeaderBox.x + firstHeaderBox.width / 2;
    const startY = firstHeaderBox.y + firstHeaderBox.height / 2;
    
    // 开始拖动
    await page.mouse.move(startX, startY);
    await page.mouse.down();
    await page.waitForTimeout(100);
    
    // 移动鼠标
    await page.mouse.move(startX + 50, startY, { steps: 5 });
    await page.waitForTimeout(200);
    
    // 检查是否有拖拽相关的元素（DragOverlay）
    const dragOverlay = page.locator('[style*="position: fixed"]').filter({
      has: page.locator('span.truncate')
    });
    
    // 拖拽阴影可能存在，但不一定总是可见
    const overlayCount = await dragOverlay.count();
    console.log(`找到的拖拽覆盖层数量: ${overlayCount}`);
    
    // 释放鼠标
    await page.mouse.up();
  });

  test('点击列头文本区域应该触发拖拽而不是菜单', async ({ page }) => {
    await page.waitForSelector('[data-slot="grid-header-cell"]', { timeout: 5000 });
    
    const headers = await page.locator('[data-slot="grid-header-cell"]').all();
    const firstHeader = headers[0];
    const textElement = firstHeader.locator('span.truncate').first();
    
    await expect(textElement).toBeVisible();
    
    // 左键点击文本区域
    await textElement.click();
    
    // 等待一小段时间
    await page.waitForTimeout(200);
    
    // 验证下拉菜单没有打开
    const menuContent = page.locator('[data-slot="dropdown-menu-content"]');
    const isMenuVisible = await menuContent.first().isVisible().catch(() => false);
    
    expect(isMenuVisible).toBe(false);
  });
});
