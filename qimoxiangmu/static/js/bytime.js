// 初始化ECharts
var chart = echarts.init(document.getElementById('chart'));

// 设置初始
var option = {
    tooltip: {
        trigger: 'axis',
        formatter: function(params) {
            var time = new Date(params[0].axisValue);
            return `${time.getFullYear()}-${(time.getMonth()+1).toString().padStart(2, '0')}-${time.getDate().toString().padStart(2, '0')} ${time.getHours().toString().padStart(2, '0')}:${time.getMinutes().toString().padStart(2, '0')}:${time.getSeconds().toString().padStart(2, '0')}<br/>` +
                params.map(function(param) {
                    return `${param.seriesName}: ${param.value[1]}`;
                }).join('<br/>');
        },
        axisPointer: {
            type: 'cross',
            label: {
                backgroundColor: '#6a7985'
            }
        },
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        padding: 8,
        textStyle: {
            color: '#fff'
        }
    },
    legend: {
        data: ['有效订单', '无效订单'],
        bottom: 10,
        textStyle: {
            color: '#333'
        }
    },
    grid: {
        left: '3%',
        right: '4%',
        bottom: '15%',
        top: '15%',
        containLabel: true
    },
    xAxis: {
        type: 'time',
        boundaryGap: false,
        axisLine: {
            lineStyle: {
                color: '#999'
            }
        },
        axisLabel: {
            formatter: function(value) {
                var date = new Date(value);
                return `${date.getHours()}:${date.getMinutes().toString().padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}`;
            }
        },
        splitLine: {
            show: true,
            lineStyle: {
                color: '#eee'
            }
        }
    },
    yAxis: {
        type: 'value',
        name: '订单数量',
        axisLine: {
            show: true,
            lineStyle: {
                color: '#999'
            }
        },
        axisLabel: {
            fontSize: 12,
            formatter: function(value) {
                if (value >= 1000) {
                    return value / 1000 + 'k';
                }
                return value;
            }
        },
        splitLine: {
            show: true,
            lineStyle: {
                color: '#eee'
            }
        },
        min: function(value) {
            return Math.floor(value.min * 0.9);
        },
        max: function(value) {
            return Math.ceil(value.max * 1.1);
        }
    },
    series: [
        {
            name: '有效订单',
            type: 'line',
            data: [],
            smooth: true,
            showSymbol: false,
            lineStyle: {
                width: 2,
                color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                    { offset: 0, color: 'rgba(52, 152, 219, 1)' },
                    { offset: 1, color: 'rgba(52, 152, 219, 0.5)' }
                ])
            },
            areaStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                    { offset: 0, color: 'rgba(52, 152, 219, 0.3)' },
                    { offset: 1, color: 'rgba(52, 152, 219, 0.1)' }
                ])
            },
            emphasis: {
                lineStyle: {
                    width: 4
                }
            }
        },
        {
            name: '无效订单',
            type: 'line',
            data: [],
            smooth: true,
            showSymbol: false,
            lineStyle: {
                width: 2,
                color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                    { offset: 0, color: 'rgba(231, 76, 60, 1)' },
                    { offset: 1, color: 'rgba(231, 76, 60, 0.5)' }
                ])
            },
            areaStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                    { offset: 0, color: 'rgba(231, 76, 60, 0.3)' },
                    { offset: 1, color: 'rgba(231, 76, 60, 0.1)' }
                ])
            },
            emphasis: {
                lineStyle: {
                    width: 4
                }
            }
        }
    ]
};

chart.setOption(option);

// WebSocket连接
var socket = io.connect('http://localhost:5000');

// 连接状态处理
socket.on('connect', function() {
    document.getElementById('connectionStatus').className = 'connection-status connected';
    document.getElementById('connectionStatus').textContent = '已连接到服务器';
    document.getElementById('loadingOverlay').style.display = 'none';
});

socket.on('disconnect', function() {
    document.getElementById('connectionStatus').className = 'connection-status disconnected';
    document.getElementById('connectionStatus').textContent = '服务器连接已断开';
});

socket.on('error', function(error) {
    console.error("WebSocket 错误：", error);
    document.getElementById('connectionStatus').className = 'connection-status disconnected';
    document.getElementById('connectionStatus').textContent = '服务器连接错误';
});

// 处理服务器推送的数据
socket.on('update_data', function(data) {
    var currentTime = new Date();
    var time = currentTime.getTime();

    // 更新最后更新时间
    updateLastUpdated();

    // 更新订单总数统计
    document.getElementById('validOrdersCount').textContent = data.order_stats.valid;
    document.getElementById('invalidOrdersCount').textContent = data.order_stats.invalid;

    // 计算趋势变化
    var validTrend = Math.round((data.order_stats.valid / (data.order_stats.valid + data.order_stats.invalid) * 100)) || 0;
    var invalidTrend = 100 - validTrend;

    document.getElementById('validOrdersTrend').textContent = '+' + validTrend + '%';
    document.getElementById('invalidOrdersTrend').textContent = '+' + invalidTrend + '%';

    // 更新折线图数据
    var validSeries = chart.getOption().series[0];
    var invalidSeries = chart.getOption().series[1];

    // 限制数据点数量，保持性能
    if (validSeries.data.length > 30) {
        validSeries.data.shift();
        invalidSeries.data.shift();
    }

    validSeries.data.push([time, data.order_stats.valid]);
    invalidSeries.data.push([time, data.order_stats.invalid]);

    // 更新图表
    chart.setOption({
        series: [validSeries, invalidSeries]
    });

    // 更新订单动态
    var updates = [];
    var updatesCount = 0;

    Object.entries(data.order_number_stats).forEach(([orderId, counts]) => {
        if (counts.Y > 0 || counts.N > 0) {
            updatesCount++;
            var timeStr = currentTime.toLocaleTimeString();
            var updateItem = document.createElement('div');
            updateItem.className = 'update-item';

            updateItem.innerHTML = `
              <div class="update-time">${timeStr}</div>
              <div class="update-content">
                <span class="update-order-id">${orderId}</span>
                <span class="update-valid">有效 +${counts.Y}</span>
                <span class="update-invalid">无效 +${counts.N}</span>
              </div>
            `;

            document.getElementById('orderUpdates').prepend(updateItem);
        }
    });

    if (updatesCount > 0) {
        document.getElementById('updatesCount').textContent = updatesCount + ' 条更新';

        // 如果没有滚动到底部，则显示新更新提示
        var updatesList = document.getElementById('orderUpdates');
        if (updatesList.scrollTop + updatesList.clientHeight < updatesList.scrollHeight - 20) {
            setTimeout(function() {
                var notification = document.createElement('div');
                notification.className = 'new-updates-notification';
                notification.textContent = '有新的订单更新';
                updatesList.appendChild(notification);

                setTimeout(function() {
                    notification.style.opacity = '0';
                    setTimeout(function() {
                        notification.remove();
                    }, 500);
                }, 3000);
            }, 500);
        } else {
            // 自动滚动到底部
            updatesList.scrollTop = updatesList.scrollHeight;
        }
    }

    // 移除旧的"暂无更新"消息
    var noUpdates = document.querySelector('.no-updates-message');
    if (noUpdates) {
        noUpdates.remove();
    }
});

// 窗口大小改变时调整图表大小
window.addEventListener('resize', function() {
    chart.resize();
});

// 初始加载时显示加载动画
window.addEventListener('load', function() {
    setTimeout(function() {
        document.getElementById('loadingOverlay').style.opacity = '0';
        setTimeout(function() {
            document.getElementById('loadingOverlay').style.display = 'none';
        }, 500);
    }, 1000);
});

// 更新时间显示函数
function updateLastUpdated() {
    const now = new Date();
    const timeString = now.toLocaleTimeString('zh-CN', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });

    // 添加数字滚动效果
    const timeElements = timeString.split(':').map(num =>
        `<span class="digital" data-prev="${num}">${num}</span>`).join(':');

    document.getElementById('lastUpdated').innerHTML =
        '最后更新: ' + timeElements;
}

// 每秒更新一次时间显示
setInterval(updateLastUpdated, 1000);