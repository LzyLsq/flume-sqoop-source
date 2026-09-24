document.addEventListener("DOMContentLoaded", function () {
    const sign_in_btn = document.querySelector("#sign-in-btn");
    const sign_up_btn = document.querySelector("#sign-up-btn");
    const container = document.querySelector(".container");
    const body = document.querySelector("body");

    sign_up_btn.addEventListener("click", () => {
        container.classList.add("sign-up-mode");
    });

    sign_in_btn.addEventListener("click", () => {
        container.classList.remove("sign-up-mode");
    });

    //添加表单提交事件
    const loginForm = document.querySelector(".sign-in-form");
    const registerForm = document.querySelector(".sign-up-form");

    loginForm.addEventListener("submit", function (e) {
        e.preventDefault();
        const username = this.querySelector('input[name="username"]').value;
        const password = this.querySelector('input[name="password"]').value;
        localStorage.setItem('username', username);
        fetch("/login", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ username, password }),
        })
            .then((response) => response.json())
            .then((data) => {
                if (data.success) {
                    // 添加淡出动画
                    body.style.opacity = "0";
                    setTimeout(() => {
                        window.location.href = "/dashboard";
                    }, 500); // 等待动画完成后再跳转
                } else {
                    alert(data.message);
                }
            })
            .catch((error) => {
                console.error("Error:", error);
            });
    });

    registerForm.addEventListener("submit", function (e) {
        e.preventDefault();
        const username = this.querySelector('input[name="username"]').value;
        const email = this.querySelector('input[name="email"]').value;
        const password = this.querySelector('input[name="password"]').value;

        fetch("/register", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ username, email, password }),
        })
            .then((response) => response.json())
            .then((data) => {
                if (data.success) {
                    // 添加淡出动画
                    body.style.opacity = "0";
                    setTimeout(() => {
                        window.location.href = "/dashboard";
                    }, 500); // 等待动画完成后再跳转
                } else {
                    alert(data.message);
                }
            })
            .catch((error) => {
                console.error("Error:", error);
            });
    });
});

