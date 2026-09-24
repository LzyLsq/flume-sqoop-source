
// 创建场景
    const scene = new THREE.Scene();//渲染器
// 创建相机，设置视角、宽高比、近裁剪面和远裁剪面
    const camera = new THREE.PerspectiveCamera(30, window.innerWidth / window.innerHeight, 0.1, 1000);
    camera.position.set(0, 2, 8);// 设置相机位置
// 创建渲染器，启用抗锯齿
    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setClearColor(0xFFFFFF,0);// 设置背景色为白色
    renderer.setSize(window.innerWidth/2.5, window.innerHeight/2.5);// 设置渲染器大小
    //renderer.domElement.classList.add('renderer-border');
    document.body.appendChild(renderer.domElement);// 将渲染器添加到文档中
// 为渲染器的canvas添加边框样式
    const style = renderer.domElement.style;
    const canvas = renderer.domElement;
    canvas.classList.add('renderer-border');// 添加边框样式
    document.body.appendChild(canvas);// 再次添加canvas（可以省略）

    //const textureLoader = new THREE.TextureLoader();
    //const earthTexture = textureLoader.load('/static/earth5.jpg');
// 创建地球的几何形状
    const earthGeometry = new THREE.SphereGeometry(1, 64, 64);
    /* const earthMaterial = new THREE.MeshStandardMaterial({
         map: earthTexture,
         roughness: 0.7,
         metalness: 0.3
     });*/
// 默认亮模式纹理路径
    let currentEarthTexturePath = '/static/earth5.jpg'; // 默认亮模式纹理路径
    const textureLoader = new THREE.TextureLoader();
// 更新地球纹理的函数

    function updateEarthTexture() {
        // 根据当前模式选择纹理路径

        if (document.body.classList.contains('dark-mode')) {
            currentEarthTexturePath = '/static/earth2.jpg';// 暗模式纹理
        } else {

            currentEarthTexturePath = '/static/earth5.jpg';// 亮模式纹理
        }
        const earthTexture = textureLoader.load(currentEarthTexturePath);// 加载纹li
        earth.material.map = earthTexture;// 更新地球材质的纹理
        earth.material.needsUpdate = true;// 标记材质需要更新
    }
// 加载初始纹理
    const earthTexture = textureLoader.load(currentEarthTexturePath);
// 创建地球的材质
    const earthMaterial = new THREE.MeshStandardMaterial({
        map: earthTexture,// 设定纹理
        roughness: 0.7,// 粗糙度
        metalness: 0.3// 金属感
    });
// 创建地球网格
    const earth = new THREE.Mesh(earthGeometry, earthMaterial);
    earth.position.set(0, 0, 0);// 设置地球位置
    scene.add(earth);// 将地球添加到场景中
// 创建卫星数组和数量
    const satellites = [];
    const satelliteCount = 200;
    for (let i = 0; i < satelliteCount; i++) {
        const satelliteGeometry = new THREE.SphereGeometry(0.05, 32, 32);// 卫星几何形状

        const color = new THREE.Color();
        color.setHSL(Math.random(), 0.75, 0.5); // 使用HSL颜色空间生成随机颜色，其中饱和度和亮度固定

        const satelliteMaterial = new THREE.MeshStandardMaterial({ color: color });
        const satellite = new THREE.Mesh(satelliteGeometry, satelliteMaterial);

        // 随机轨道半径
        const orbitRadius = 3+Math.random() * 10; // 轨道半径在2到12之间随机
        // 随机方位角
        const theta = Math.random() * Math.PI * 4;
        // 随机极角
        const phi = Math.acos(Math.random() * 2 - 1);
        //生成位置
        satellite.position.x = orbitRadius * Math.sin(phi) * Math.cos(theta);
        satellite.position.y = orbitRadius * Math.sin(phi) * Math.sin(theta);
        satellite.position.z = orbitRadius * Math.cos(phi);

        scene.add(satellite);
        satellites.push(satellite);
    }

    /*const starFieldGeometry = new THREE.PlaneGeometry(window.innerWidth, window.innerHeight);
    const starFieldMaterial = new THREE.MeshBasicMaterial({ map: starFieldTexture, side: THREE.DoubleSide });
    const starField = new THREE.Mesh(starFieldGeometry, starFieldMaterial);
    starField.position.z = -5; // 将平面放置在相机的后方
    scene.add(starField);*/
   //粒子
    const particleGeometry = new THREE.BufferGeometry();
    const particleMaterial = new THREE.PointsMaterial({
        color: 0x00ffff, size: 0.05, blending: THREE.AdditiveBlending, transparent: true
    });

    const particles = [];
    for (let i = 0; i < 1000; i++) {
        const theta = Math.random() * Math.PI * 2;
        const phi = Math.acos((Math.random() * 2) - 1);
        const r = 1.05; // 半径略大于地球
        particles.push(
            r * Math.sin(phi) * Math.cos(theta),
            r * Math.sin(phi) * Math.sin(theta),
            r * Math.cos(phi)
        );
    }
    particleGeometry.setAttribute('position', new THREE.Float32BufferAttribute(particles, 3));
    const particlePoints = new THREE.Points(particleGeometry, particleMaterial);
    scene.add(particlePoints);

    //星空背景点云
    const starGeometry = new THREE.BufferGeometry();
    const stars = [];
    const starCount = 50000;
    const maxDistance = 1000; // 最大距离

    for (let i = 0; i < starCount; i++) {
        const theta = Math.random() * Math.PI * 2; // 0到2π之间，方位角
        const phi = Math.acos(Math.random() * 2 - 1); // -1到1之间，生成-π/2到π/2之间的角度，覆盖整个上半球和下半球
        const r = maxDistance * Math.random()*0.1; // 星星距离原点的距离，最大为maxDistance

        const x = r * Math.sin(phi) * Math.cos(theta);
        const y = r * Math.sin(phi) * Math.sin(theta);
        const z = r * Math.cos(phi);

        stars.push(x, y, z);

    }

    starGeometry.setAttribute('position', new THREE.Float32BufferAttribute(stars, 3));
    const starMaterial = new THREE.PointsMaterial({
        color: 0xffffff,
        size: 0.005,
        transparent: true,
        opacity: 0.9
    });
    const starField = new THREE.Points(starGeometry, starMaterial);
    scene.add(starField);


   //流线
    const earthRadius = 1;
    const flowLines = [];
    const curveMaterial = new THREE.LineBasicMaterial({ color: 0xffa500, opacity: 0.8, transparent: true });
    const fixedPoints = [
        new THREE.Vector3(earthRadius, 0, 0),
        new THREE.Vector3(0, earthRadius, 0),
        new THREE.Vector3(0, 0, earthRadius)
    ];
    //挑随机点
    function createFlowLine() {
        const point1 = getRandomPositionOnSphere(1.02);
        const point2 = getRandomPositionOnSphere(1.02);
        const point3 = getRandomPositionOnSphere(1.02);
        const point4 = getRandomPositionOnSphere(1.02);

        const curve = new THREE.CatmullRomCurve3([
            new THREE.Vector3(...point1),
            new THREE.Vector3(...point2),
            new THREE.Vector3(...point3),
            new THREE.Vector3(...point4),



        ]);
        const points = curve.getPoints(100);
        //const tubeGeometry = new THREE.TubeGeometry(curve, 64, 0.05, 8, false);

        const geometry = new THREE.BufferGeometry().setFromPoints(points);



        const line = new THREE.Line(geometry, curveMaterial);
        scene.add(line);

        flowLines.push({ line, points, index: 0 });
    }
    //大小，位置
    function getRandomPositionOnSphere(radius) {
        const theta = Math.random() * Math.PI * 2;
        const phi = Math.acos((Math.random() * 2) - 1);
        return [
            radius * Math.sin(phi) * Math.cos(theta),
            radius * Math.sin(phi) * Math.sin(theta),
            radius * Math.cos(phi)
        ];
    }

    fixedPoints.forEach(point => {
        for (let i = 0; i < 20; i++) createFlowLine(point);
    });

    function updateFlowLines() {
        flowLines.forEach((flow) => {
            const { line, points, index } = flow;

            const visiblePoints = points.slice(index, index + 12);
            const geometry = new THREE.BufferGeometry().setFromPoints(visiblePoints);
            line.geometry.dispose();
            line.geometry = geometry;

            const progress = index / points.length;
            const color = new THREE.Color().setHSL(progress, 1.0, 0.5); // HSL颜色渐变
            line.material.color.set(color);
            line.material.opacity = 0.8 - progress * 0.6;
            flow.index = (index + 0.5) % points.length;
        });
    }

    function updateParticles() {
        const radius = 1.15;

        particleGeometry.attributes.position.array.forEach((_, index) => {
            const theta = Math.random() * Math.PI * 2;
            const phi = Math.acos((Math.random() * 2) - 1);
            const x = radius * Math.sin(phi) * Math.cos(theta);
            const y = radius * Math.sin(phi) * Math.sin(theta);
            const z = radius * Math.cos(phi);

            particleGeometry.attributes.position.setXYZ(index, x, y, z);
        });

        particleGeometry.attributes.position.needsUpdate = true;
    }
    const ambientLight = new THREE.AmbientLight(0xffffff, 3);
    scene.add(ambientLight);
    const pointLight = new THREE.PointLight(0xffffff, 3);
    pointLight.position.set(1, 1, 1);
    scene.add(pointLight);

    const controls = new THREE.OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;

    window.addEventListener('resize', () => {
        camera.aspect = window.innerWidth / window.innerHeight;
        camera.updateProjectionMatrix();
        renderer.setSize(window.innerWidth/2.5, window.innerHeight/2.5);
    });

   //每点一下加流线
    window.addEventListener('click', () => {
        createFlowLine();
    });

    const satelliteStates = [];
    satellites.forEach((satellite, index) => {
        const orbitRadius = 2 + index * 2;
        satelliteStates.push({
            orbitRadius,
            angle: Math.random() * Math.PI * 2,
            angleSpeed: 0.001 + (index * 0.0005)
        });
    });
    window.onload = function () {

        if (!document.body.classList.contains('dark-mode')) {
            document.body.classList.add('light-mode');
        }
        updateEarthTexture();
    };

    //动画开始
    function animate() {
        requestAnimationFrame(animate);
        //地球自转
        earth.rotation.y += 0.001;
        earth.rotation.x += 0.0005;
        earth.rotation.z += 0.0005;
        //卫星转
        satelliteStates.forEach((state, index) => {
            const {
                orbitRadius,
                angle,
                angleSpeed
            } = state;
            const newAngle = angle + angleSpeed; // 更新角度

            satellites[index].position.x = orbitRadius * Math.cos(newAngle);
            satellites[index].position.y = orbitRadius * Math.sin(newAngle);
            satellites[index].position.z = 0; // 保持在XY平面上

            state.angle = newAngle; // 更新状态
        });
        //星云转
        starField.rotation.y -= 0.0001;
        //粒子
        particlePoints.rotation.y += 0.001;
        //启动
        updateFlowLines();
        updateParticles();
        controls.update();
        renderer.render(scene, camera);
    }

    animate();
