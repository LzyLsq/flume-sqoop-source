// 添加选中反馈
const buttons = document.querySelectorAll('.hover-effect');
buttons.forEach(button => {
    button.addEventListener('click', () => {
        // 移除其他按钮的选中状态
        buttons.forEach(btn => {
            btn.classList.remove('selected');
            btn.blur();
        });
        // 添加当前按钮的选中状态
        button.classList.add('selected');
    });
});

// 为iframe添加加载事件
const iframe = document.querySelector('iframe');
const loading = document.querySelector('.loading');
iframe.addEventListener('load', () => {
    loading.style.display = 'none';
});
iframe.addEventListener('error', () => {
    loading.style.display = 'none';
});
// 在页面加载时显示加载动画
window.onload = function() {
    loading.style.display = 'block';
    setTimeout(() => {
        loading.style.display = 'none';
    }, 2000);
};
function updateTime() {
    const now = new Date();
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');
    const timeString = `${hours}:${minutes}:${seconds}`;
    document.getElementById('clock').textContent = timeString;
}

// 初始调用
updateTime();
// 每秒更新一次时间
setInterval(updateTime, 1000);


// 从localStorage中获取用户名
const username = localStorage.getItem('username');
// 如果用户名存在，显示欢迎消息
if (username) {
    document.getElementById('welcomeMessage').textContent = `欢迎, ${username}!`;
} else {
    document.getElementById('welcomeMessage').textContent = '您尚未登录。';
}
function showLogoutPrompt(event) {
    // 阻止默认行为（即直接跳转到href的链接）
    event.preventDefault();

    // 弹出提示框
    if (confirm("你确定要退出登录吗？")) {
        // 如果用户点击确认，则跳转到logout页面
        window.location.href = "/logout";
    }
}
function search() {
    let query = document.getElementById('search-input').value;
    if (query) {
        fetch(`http://127.0.0.1:5000/search?query=${query}`) // 修改为Python后端的地址
            .then(response => response.json())
            .then(data => {
                let resultsDiv = document.getElementById('results');
                resultsDiv.innerHTML = '';
                if (data.length === 0) {
                    resultsDiv.innerHTML = '<p>未找到匹配结果</p>';
                } else {
                    data.forEach(item => {
                        let div = document.createElement('div');
                        div.classList.add('result-item');
                        div.textContent = item.name;
                        resultsDiv.appendChild(div);
                    });
                }
            })
            .catch(error => console.error('Error:', error));
    } else {
        alert('请输入搜索内容');
    }
}
