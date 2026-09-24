
    // 实时时间
    //定义了一个名为 showTime 的函数
    function showTime() {
        var date = new Date();//创建一个 Date 对象，用于获取当前的日期和时间。
        var time = date.toLocaleTimeString();//将时间格式化为本地时间字符串。
        document.getElementById('timeDisplay').innerHTML = time;//将格式化后的时间显示在网页上，定位到 id 为 timeDisplay 的元素中
    }

    setInterval(showTime, 1000);//每隔 1000 毫秒（即 1 秒）调用一次 showTime 函数，以实现实时更新时间的效果。
