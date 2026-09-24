
    var socket = io.connect('http://localhost:5000');
    socket.on('connect', function () {
        console.log("WebSocket 已连接");
        document.getElementById('loading').style.display = 'none';
    });

    socket.on('disconnect', function () {
        console.warn("WebSocket 已断开");
        document.getElementById('loading').style.display = 'block';
        setTimeout(() => {
            console.log("尝试重连 WebSocket...");
            socket.connect(); // 重连机制
        }, 5000);
    });

    socket.on('error', function (error) {
        console.error("WebSocket 错误：", error);
    });


    socket.on('update_data', function (data) {
        console.log("收到服务器推送数据：", data);

        // 处理
        var orderIds = Object.keys(data.order_number_stats);//键值订单号
        var validCounts = orderIds.map(id => data.order_number_stats[id].Y);
        var invalidCounts = orderIds.map(id => data.order_number_stats[id].N);

        var categories = Object.keys(data.order_category_stats);//键值种类号
        var categoryCounts = categories.map(category =>
            data.order_category_stats[category].Y + data.order_category_stats[category].N
        );

        var validCountsByCategory = categories.map(category => data.order_category_stats[category].Y);
        var invalidCountsByCategory = categories.map(category => data.order_category_stats[category].N);

        var pieData = categories.map(category => ({
            name: category,
            value: data.order_category_stats[category].Y + data.order_category_stats[category].N
        }));
        // 更新图表数据
        //1.所有有效和无效订单数量总和
        if (window.chart1 && typeof window.chart1.setOption === 'function') {
            chart1.setOption({
                series: [{data: [data.order_stats.valid, data.order_stats.invalid]}]
            });
        }
        //2.各个订单号各自的有效和无效数量
        if (window.chart2 && typeof window.chart2.setOption === 'function') {
            chart2.setOption({
                xAxis: {data: orderIds},
                series: [
                    {name: '有效', data: validCounts},
                    {name: '无效', data: invalidCounts}
                ]
            });
        }
        //3.所有订单类别的数量
        if (window.chart3 && typeof window.chart3.setOption === 'function') {
            chart3.setOption({
                xAxis: {data: categories},
                series: [{data: categoryCounts}]
            });
        }
        //4.不同类别商品有效与无效的数量
        if (window.chart4 && typeof window.chart4.setOption === 'function') {
            chart4.setOption({
                xAxis: {data: categories},
                series: [
                    {name: '有效', data: validCountsByCategory},
                    {name: '无效', data: invalidCountsByCategory}
                ]
            });
        }
        //5.各类商品数量比例
        if (window.chart5 && typeof window.chart5.setOption === 'function') {
            chart5.setOption({
                series: [{data: pieData}]
            });
        }
        // 显示数据更新通知
        /* document.getElementById('updateNotification').style.display = 'block';
         setTimeout(() => {
             document.getElementById('updateNotification').style.display = 'none';
         }, 2000);*/
    });
    const { createProxyMiddleware } = require('http-proxy-middleware');

    app.use(
        '/api',
        createProxyMiddleware({
            target: 'http://39.105.42.89',
            changeOrigin: true,
        })
    );