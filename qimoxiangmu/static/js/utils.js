// 函数，可添加新功能
function getCurrentTime() {
    const now = new Date(); // 创建一个 Date 对象，表示当前日期和时间

    // 获取当前小时，并将其转换为字符串，确保长度为2位（不足2位前面补零）
    const hours = now.getHours().toString().padStart(2, '0');

    // 获取当前分钟，并将其转换为字符串，确保长度为2位（不足2位前面补零）
    const minutes = now.getMinutes().toString().padStart(2, '0');

    // 获取当前秒钟，并将其转换为字符串，确保长度为2位（不足2位前面补零）
    const seconds = now.getSeconds().toString().padStart(2, '0');

    // 返回格式化后的时间字符串，格式为 "HH:MM:SS"
    return `${hours}:${minutes}:${seconds}`;
}