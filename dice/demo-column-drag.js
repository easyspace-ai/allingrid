// 浏览器控制台演示脚本
// 在浏览器中打开 http://localhost:5173，然后打开开发者工具控制台，粘贴并运行此脚本

(async function demonstrateColumnDragAndRightClick() {
  console.log('🚀 开始演示列拖动排序和右键菜单功能...\n');

  // 等待表格加载
  await new Promise(resolve => {
    const checkTable = setInterval(() => {
      const grid = document.querySelector('[data-slot="grid"]');
      if (grid) {
        clearInterval(checkTable);
        resolve();
      }
    }, 100);
  });

  console.log('✅ 表格已加载\n');

  // 获取所有列头
  const headers = Array.from(document.querySelectorAll('[data-slot="grid-header-cell"]'));
  console.log(`📋 找到 ${headers.length} 个列头\n`);

  if (headers.length < 2) {
    console.error('❌ 需要至少2个列头才能进行拖拽测试');
    return;
  }

  // 获取第一个和第二个列头的文本
  const firstHeader = headers[0];
  const secondHeader = headers[1];
  const firstHeaderText = firstHeader.querySelector('span.truncate')?.textContent || '';
  const secondHeaderText = secondHeader.querySelector('span.truncate')?.textContent || '';

  console.log(`📌 第一个列头: "${firstHeaderText}"`);
  console.log(`📌 第二个列头: "${secondHeaderText}"\n`);

  // 测试1: 右键菜单
  console.log('🧪 测试1: 右键菜单功能');
  console.log('   在第一个列头上右键点击...\n');
  
  const firstHeaderRect = firstHeader.getBoundingClientRect();
  const clickX = firstHeaderRect.left + firstHeaderRect.width / 2;
  const clickY = firstHeaderRect.top + firstHeaderRect.height / 2;

  // 模拟右键点击
  const contextMenuEvent = new MouseEvent('contextmenu', {
    bubbles: true,
    cancelable: true,
    clientX: clickX,
    clientY: clickY,
    button: 2
  });
  
  firstHeader.dispatchEvent(contextMenuEvent);

  await new Promise(resolve => setTimeout(resolve, 500));

  // 检查菜单是否打开
  const menu = document.querySelector('[data-slot="dropdown-menu-content"]');
  if (menu) {
    const menuRect = menu.getBoundingClientRect();
    console.log(`✅ 菜单已打开`);
    console.log(`   菜单位置: (${menuRect.left.toFixed(0)}, ${menuRect.top.toFixed(0)})`);
    console.log(`   点击位置: (${clickX.toFixed(0)}, ${clickY.toFixed(0)})`);
    console.log(`   位置偏移: x=${Math.abs(menuRect.left - clickX).toFixed(0)}px, y=${Math.abs(menuRect.top - clickY).toFixed(0)}px\n`);
    
    // 关闭菜单
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
    await new Promise(resolve => setTimeout(resolve, 200));
  } else {
    console.log('❌ 菜单未打开\n');
  }

  // 测试2: 拖拽功能说明
  console.log('🧪 测试2: 拖拽功能');
  console.log('   请在浏览器中手动执行以下操作：');
  console.log('   1. 点击并拖动第一个列头的文本区域');
  console.log('   2. 将列头拖动到第二个列头的位置');
  console.log('   3. 释放鼠标');
  console.log('   4. 观察列顺序是否改变\n');

  console.log('📝 预期行为：');
  console.log('   ✓ 拖动时显示半透明灰色阴影');
  console.log('   ✓ 阴影高度为整个表格的高度');
  console.log('   ✓ 被覆盖的列会高亮边线');
  console.log('   ✓ 列只能在横向移动，Y轴锁定');
  console.log('   ✓ 释放后列顺序改变\n');

  console.log('✨ 演示完成！');
})();

