const socket = io.connect('http://localhost:5000');
const textContainer = document.getElementById('analysisTextContainer');
const chartCanvas = document.getElementById('analysisChart');

// 添加加载动画
const loader = document.createElement('div');
loader.className = 'analysis-item';
loader.innerHTML = `<p style="text-align:center;color:var(--text-secondary)">⏳ 正在连接实时数据...</p>`;
textContainer.appendChild(loader);

// 初始化图表
const ctx = chartCanvas.getContext('2d');
let chart = new Chart(ctx, {
    type: 'bar',
    data: {
        labels: [],
        datasets: []
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: 'top',
                labels: {
                    font: {
                        family: "'Roboto', 'PingFang SC', 'Microsoft YaHei', sans-serif",
                        size: 14,
                        weight: 'normal',
                        lineHeight: 1.2
                    },
                    color: '#00f7ff', // 设置图例文字颜色为亮蓝色
                    padding: 20,
                    usePointStyle: false,
                    boxWidth: 12
                }
            },
            tooltip: {
                mode: 'index',
                intersect: false,
                backgroundColor: 'rgba(0, 0, 0, 0.7)',
                titleFont: {
                    family: "'Roboto', 'PingFang SC', 'Microsoft YaHei', sans-serif",
                    size: 14,
                    weight: 'bold'
                },
                titleSpacing: 10,
                titleMarginBottom: 10,
                bodyFont: {
                    family: "'Roboto', 'PingFang SC', 'Microsoft YaHei', sans-serif",
                    size: 14
                },
                displayColors: true,
                borderColor: '#00f7ff',
                borderWidth: 1,
                padding: 15,
                cornerRadius: 5,
                callbacks: {
                    label: function(context) {
                        return context.dataset.label + ': ' + context.parsed.y;
                    }
                }
            }
        },
        scales: {
            x: {
                grid: {
                    display: false
                },
                ticks: {
                    font: {
                        family: "'Roboto', 'PingFang SC', 'Microsoft YaHei', sans-serif",
                        size: 12
                    },
                    color: '#00f7ff' // 设置X轴文字颜色为亮蓝色
                },
                title: {
                    display: true,
                    text: '时间',
                    font: {
                        family: "'Roboto', 'PingFang SC', 'Microsoft YaHei', sans-serif",
                        size: 14,
                        weight: 'bold'
                    },
                    color: '#00f7ff' // 设置X轴标题颜色为亮蓝色
                }
            },
            y: {
                beginAtZero: true,
                grid: {
                    color: 'rgba(0, 0, 0, 0.05)'
                },
                ticks: {
                    font: {
                        family: "'Roboto', 'PingFang SC', 'Microsoft YaHei', sans-serif",
                        size: 12
                    },
                    color: '#00f7ff' // 设置Y轴文字颜色为亮蓝色
                },
                title: {
                    display: true,
                    text: '订单数量',
                    font: {
                        family: "'Roboto', 'PingFang SC', 'Microsoft YaHei', sans-serif",
                        size: 14,
                        weight: 'bold'
                    },
                    color: '#00f7ff' // 设置Y轴标题颜色为亮蓝色
                }
            }
        }
    }
});

socket.on('connect', () => {
    loader.remove();
    console.log('Connected to server');
});

socket.on('update_data', (data) => {
    if (data.kimi_analysis_result) {
        const now = new Date();
        const timeString = now.toLocaleString();

        // 更新文本数据
        const analysisItem = document.createElement('div');
        analysisItem.className = 'analysis-item';
        analysisItem.innerHTML = `
                    <p>${data.kimi_analysis_result}</p>
                    <div class="analysis-time">${timeString}</div>
                `;

        textContainer.prepend(analysisItem);

        // 移除加载提示（如果有）
        if (document.querySelector('.no-data')) {
            document.querySelector('.no-data').remove();
        }

        // 保持最多30条记录
        const items = textContainer.getElementsByClassName('analysis-item');
        if (items.length > 30) {
            textContainer.removeChild(items[items.length - 1]);
        }
    }

    if (data.order_category_stats) {
        updateChart(data.order_category_stats);
    }
});

function updateChart(categoryStats) {
    const now = new Date().toLocaleString();

    // 如果是第一次初始化图表
    if (chart.data.labels.length === 0) {
        // 为每个类别创建两个数据集（有效和无效）
        const datasets = [];
        for (const category of Object.keys(categoryStats)) {
            datasets.push({
                label: `${category} 有效订单`,
                data: [],
                backgroundColor: `rgba(${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, 0.7)`,
                borderColor: `rgba(${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, 1)`,
                borderWidth: 1
            });
            datasets.push({
                label: `${category} 无效订单`,
                data: [],
                backgroundColor: `rgba(${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, 0.7)`,
                borderColor: `rgba(${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, ${Math.floor(Math.random() * 255)}, 1)`,
                borderWidth: 1
            });
        }
        chart.data.datasets = datasets;
    }

    // 更新图表数据
    const newLabels = chart.data.labels.slice();
    newLabels.push(now);

    // 为每个类别更新有效和无效订单数据
    for (const [index, category] of Object.entries(Object.keys(categoryStats))) {
        const validDatasetIndex = index * 2;
        const invalidDatasetIndex = index * 2 + 1;

        chart.data.datasets[validDatasetIndex].data.push(categoryStats[category].Y);
        chart.data.datasets[invalidDatasetIndex].data.push(categoryStats[category].N);
    }

    chart.data.labels = newLabels;

    // 保持图表最多显示10个数据点
    if (chart.data.labels.length > 10) {
        chart.data.labels.shift();
        chart.data.datasets.forEach(dataset => {
            dataset.data.shift();
        });
    }

    chart.update();
}

// 处理无数据情况
setTimeout(() => {
    if (textContainer.children.length === 0) {
        const emptyState = document.createElement('div');
        emptyState.className = 'analysis-item no-data';
        emptyState.innerHTML = `<p style="text-align:center;color:var(--text-secondary)">📭 暂无实时数据</p>`;
        textContainer.appendChild(emptyState);
    }
}, 5000);