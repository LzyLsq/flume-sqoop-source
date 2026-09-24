

// 切换按钮
    document.getElementById('themeToggle').addEventListener('click', function () {
        // document.body.classList.toggle('dark-mode');
        document.body.classList.toggle('light-mode');
        document.body.classList.toggle('dark-mode');

        var icon = document.getElementById('themeIcon');
        // 主题切换图片，注意路径
        icon.src = document.body.classList.contains('dark-mode') ?
            '/static/moon.png' :
            '/static/son.png';
        updateEarthTexture();

    });

    window.onload = function () {

        // 这里假设默认是明亮模式，可根据实际需求修改判断逻辑，比如从本地存储读取之前设置的主题（这里不做本地存储）
        if (!document.body.classList.contains('dark-mode')) {
            document.body.classList.add('light-mode');
        }
        updateEarthTexture(); // 页面加载时初始化地球纹理（图片）
    };


