
// 图表初始化（建实例）
function initializeChart(id, options) {
    const chart = echarts.init(document.getElementById(id));
    chart.setOption(options);
    return chart;
}

var chart1 = initializeChart('chart1', {
    title: {
        text: '1.所有有效和无效订单数量总和',
        textStyle: {
            color: '#fff', // 字体颜色
            fontSize: '2vw', // 字体大小保持不变
            fontWeight: 'bold', // 字体加粗
            fontFamily: 'Arial', // 字体类型
            shadowColor: 'rgba(0, 255, 255, 0.5)', // 文字阴影颜色
            shadowBlur: 10, // 文字阴影模糊大小
            shadowOffsetX: 4, // 文字阴影水平偏移
            shadowOffsetY: 4, // 文字阴影垂直偏移
            textBorderColor: 'rgba(0, 255, 255, 0.5)', // 文字描边颜色
            textBorderWidth: 1 // 文字描边宽度
        },
        emphasis: { // 鼠标悬停时的高亮效果
            textStyle: {
                color: '#ff0', // 高亮颜色
                shadowBlur: 0 // 取消阴影
            }
        }
    },
    tooltip: {
        trigger: 'axis',
        axisPointer: {
            type: 'shadow' // 鼠标悬浮时的指示器类型
        }
    },
    xAxis: {
        type: 'category',
        data: ['有效', '无效'],
        axisLabel: {
            fontSize: 5, // 轴标签字体大小保持不变
            color: '#00D1FF', // 蓝色，科技感
            fontWeight: 'bold' // 字体加粗
        },
        axisLine: {
            lineStyle: {
                color: '#00D1FF', // 蓝色，科技感
            }
        }
    },
    yAxis: {
        type: 'value',
        axisLabel: {
            fontSize: 5, // 轴标签字体大小保持不变
            color: '#00D1FF', // 蓝色，科技感
            fontWeight: 'bold' // 字体加粗
        },
        axisLine: {
            lineStyle: {
                color: '#00D1FF', // 蓝色，科技感
            }
        }
    },
    series: [{
        data: [0, 0],
        type: 'bar',
        itemStyle: {
            borderColor: '#00D1FF', // 蓝色，科技感
            borderWidth: 2,
            color: new echarts.graphic.LinearGradient(
                0, 0, 0, 1,
                [
                    {offset: 0, color: '#83bff6'},
                    {offset: 1, color: '#188df0'}
                ]
            )
        }
    }]
});



var chart2 = initializeChart('chart2', {
    title: {
        text: '2.各个订单号各自的有效和无效数量',
        textStyle: {
            color: '#fff', // 字体颜色
            fontSize: '2vw', // 字体大小保持不变
            fontWeight: 'bold', // 字体加粗
            fontFamily: 'Arial', // 字体类型
            shadowColor: 'rgba(0, 255, 255, 0.5)', // 文字阴影颜色
            shadowBlur: 10, // 文字阴影模糊大小
            shadowOffsetX: 4, // 文字阴影水平偏移
            shadowOffsetY: 4, // 文字阴影垂直偏移
            textBorderColor: 'rgba(0, 255, 255, 0.5)', // 文字描边颜色
            textBorderWidth: 1 // 文字描边宽度
        }
    },
    tooltip: {
        trigger: 'axis',
        axisPointer: {
            type: 'line',
            lineStyle: {
                color: '#00D1FF' // 蓝色，科技感
            }
        }
    },
    xAxis: {
        type: 'category',
        data: [],
        axisLabel: {
            fontSize: 5, // 轴标签字体大小保持不变
            color: '#00D1FF', // 蓝色，科技感
            fontWeight: 'bold' // 字体加粗
        },
        axisLine: {
            lineStyle: {
                color: '#00D1FF' // 蓝色，科技感
            }
        }
    },
    yAxis: {
        type: 'value',
        axisLabel: {
            fontSize: 5, // 轴标签字体大小保持不变
            color: '#00D1FF', // 蓝色，科技感
            fontWeight: 'bold' // 字体加粗
        },
        axisLine: {
            lineStyle: {
                color: '#00D1FF' // 蓝色，科技感
            }
        }
    },
    series: [
        {
            name: '有效',
            type: 'line',
            data: [],
            itemStyle: {
                color: '#00D1FF' // 蓝色，科技感
            },
            lineStyle: {
                width: 2,
                type: 'solid',
                color: new echarts.graphic.LinearGradient(
                    0, 0, 1, 0,
                    [
                        {offset: 0, color: '#83bff6'},
                        {offset: 1, color: '#188df0'}
                    ]
                )
            },
            areaStyle: {} // 区域填充样式
        },
        {
            name: '无效',
            type: 'line',
            data: [],
            itemStyle: {
                color: '#FF4757' // 红色，科技感
            },
            lineStyle: {
                width: 2,
                type: 'solid',
                color: new echarts.graphic.LinearGradient(
                    0, 0, 1, 0,
                    [
                        {offset: 0, color: '#FFB8E2'},
                        {offset: 1, color: '#FF7875'}
                    ]
                )
            },
            areaStyle: {} // 区域填充样式
        }
    ]
});

var chart3 = initializeChart('chart3', {
    title: {
        text: '3.所有订单类别的数量',
        textStyle: {
            color: '#fff', // 字体颜色
            fontSize: '2vw', // 字体大小保持不变
            fontWeight: 'bold', // 字体加粗
            fontFamily: 'Arial', // 字体类型
            shadowColor: 'rgba(0, 0, 0, 0.5)', // 文字阴影颜色
            shadowBlur: 10, // 文字阴影模糊大小
            shadowOffsetX: 4, // 文字阴影水平偏移
            shadowOffsetY: 4, // 文字阴影垂直偏移
        }
    },
    tooltip: {
        trigger: 'axis',
        axisPointer: {
            type: 'shadow' // 鼠标悬浮时的指示器类型
        }
    },
    xAxis: {
        type: 'category',
        data: [],
        axisLabel: {
            fontSize: 5, // 轴标签字体大小
            color: '#00D1FF', // 蓝色，科技感
            fontWeight: 'bold' // 字体加粗
        },
        axisLine: {
            lineStyle: {
                color: '#00D1FF', // 蓝色，科技感
            }
        }
    },
    yAxis: {
        type: 'value',
        axisLabel: {
            fontSize: 5, // 轴标签字体大小
            color: '#00D1FF', // 蓝色，科技感
            fontWeight: 'bold' // 字体加粗
        },
        axisLine: {
            lineStyle: {
                color: '#00D1FF', // 蓝色，科技感
            }
        }
    },
    series: [{
        data: [],
        type: 'bar',
        itemStyle: {
            color: new echarts.graphic.LinearGradient(
                0, 0, 0, 1,
                [
                    {offset: 0, color: '#83bff6'},
                    {offset: 1, color: '#188df0'}
                ]
            )
        },
        emphasis: {
            itemStyle: {
                shadowBlur: 10,
                shadowOffsetX: 0,
                shadowOffsetY: 0,
                borderWidth: 0
            }
        },
        barWidth: '60%', // 设置柱状图的宽度
        animationEasing: 'elasticOut', // 设置动画效果
        animationDelay: function (idx) {
            return Math.random() * 200;
        }
    }]
});

var chart4 = initializeChart('chart4', {
    title: { text: '4.不同类别商品有效与无效的数量',
        textStyle: {
            color: '#fff', // 字体颜色
            fontSize: '2vw', // 字体大小
            fontWeight: 'bold', // 字体加粗
            fontFamily: 'Arial', // 字体类型
            shadowColor: 'rgba(0, 255, 255, 0.5)', // 文字阴影颜色
            shadowBlur: 10, // 文字阴影模糊大小
            shadowOffsetX: 4, // 文字阴影水平偏移
            shadowOffsetY: 4, // 文字阴影垂直偏移
            textBorderColor: 'rgba(0, 255, 255, 0.5)', // 文字描边颜色
            textBorderWidth: 1 // 文字描边宽度
        }
    },
    tooltip: { trigger: 'axis',
        axisPointer: {
            type: 'line',
            lineStyle: {
                color: '#00D1FF', // 蓝色，科技感
            }
        }
    },
    xAxis: { type: 'category', data: [] ,axisLabel: {
            fontSize: 5,// 这里设置 x 轴标签的字体大小
            color: '#00D1FF', // 蓝色，科技感
            fontWeight: 'bold', // 字体加粗
            itemStyle: { borderColor: '#fff', borderWidth: 2 }
        },
        axisLine: {
            lineStyle: {
                color: '#00D1FF', // 蓝色，科技感
            }
        }

    },
    yAxis: { type: 'value' ,axisLabel: {
            fontSize: 5,//这里设置 x 轴标签的字体大小
            color: '#00D1FF', // 蓝色，科技感
            fontWeight: 'bold', // 字体加粗
            itemStyle: { borderColor: '#fff', borderWidth: 2 },
        },
        axisLine: {
            lineStyle: {
                color: '#00D1FF', // 蓝色，科技感
            }
        }

    },
    series: [
        { name: '有效', type: 'line', data: [],
            itemStyle: {
                color: '#00D1FF', // 蓝色，科技感
            },
            lineStyle: {
                width: 2,
                type: 'solid',
                color: new echarts.graphic.LinearGradient(
                    0, 0, 1, 0,
                    [
                        {offset: 0, color: '#83bff6'},
                        {offset: 1, color: '#188df0'}
                    ]
                )
            },
            areaStyle: {} // 区域填充样式
        },
        { name: '无效', type: 'line', data: [],
            itemStyle: {
                color: '#FF4757', // 红色，科技感
            },
            lineStyle: {
                width: 2,
                type: 'solid',
                color: new echarts.graphic.LinearGradient(
                    0, 0, 1, 0,
                    [
                        {offset: 0, color: '#FFB8E2'},
                        {offset: 1, color: '#FF7875'}
                    ]
                )
            },
            areaStyle: {} // 区域填充样式

        }
    ]
});



var chart5 = initializeChart('chart5', {
    title: { text: '5.各类商品数量比例' ,
        textStyle: {
            color: '#fff', // 字体颜色
            fontSize: '2vw', // 字体大小
            fontWeight: 'bold', // 字体加粗
            fontFamily: 'Arial', // 字体类型
            shadowColor: 'rgba(0, 255, 255, 0.5)', // 文字阴影颜色
            shadowBlur: 10, // 文字阴影模糊大小
            shadowOffsetX: 4, // 文字阴影水平偏移
            shadowOffsetY: 4, // 文字阴影垂直偏移
            textBorderColor: 'rgba(0, 255, 255, 0.5)', // 文字描边颜色
            textBorderWidth: 1 // 文字描边宽度
        }
    },
    tooltip: { trigger: 'item',
        formatter: "{a} <br/>{b} : {c} ({d}%)"
    },
    series: [{
        name: '分类',
        type: 'pie',
        radius: '50%',
        data: [],
        itemStyle: {
            borderColor: '#fff', // 扇区边框颜色
            borderWidth: 2, // 扇区边框宽度
            shadowColor: 'rgba(0, 0, 0, 0.5)', // 扇区阴影颜色
            shadowBlur: 10, // 扇区阴影模糊大小
            shadowOffsetX: 0, // 扇区阴影水平偏移
            shadowOffsetY: 0, // 扇区阴影垂直偏移
        },
        emphasis: {
            itemStyle: {
                shadowBlur: 10, // 扇区阴影模糊大小
                shadowOffsetX: 0, // 扇区阴影水平偏移
                shadowOffsetY: 0, // 扇区阴影垂直偏移
                borderWidth: 2 // 扇区边框宽度
            }
        },
        label: {
            show: true,
            position: 'outside',
            formatter: '{b}\n{d}%',
            color: '#fff', // 标签字体颜色
            fontSize: 14, // 标签字体大小
            fontWeight: 'bold', // 标签字体加粗
            shadowColor: 'rgba(0, 0, 0, 0.5)', // 标签阴影颜色
            shadowBlur: 10, // 标签阴影模糊大小
            shadowOffsetX: 4, // 标签阴影水平偏移
            shadowOffsetY: 4 // 标签阴影垂直偏移
        },
        labelLine: {
            show: true,
            length: 20,
            length2: 30,
            smooth: 0.5,
            lineStyle: {
                color: 'rgba(255, 255, 255, 0.5)' // 标签线颜色
            }
        },
        // 添加渐变色
        color: new echarts.graphic.LinearGradient(
            0, 0, 0, 1,
            [
                {offset: 0, color: '#83bff6'},
                {offset: 0.5, color: '#188df0'},
                {offset: 1, color: '#188df0'}
            ]
        )

    }]
});




