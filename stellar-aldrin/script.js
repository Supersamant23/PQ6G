/* ============================================
   PQ6G Presentation — Slide Navigation Engine
   ============================================ */

(function () {
  let current = 0;
  const slides  = document.querySelectorAll('.slide');
  const total   = slides.length;
  const wrapper = document.getElementById('slides-wrapper');
  const sidebar = document.querySelectorAll('.chapter-item');
  const counter = document.getElementById('slide-counter');
  const glow    = document.querySelector('.bg-glow');

  // Accent colour per slide (used for glow + sidebar highlight)
  const accentMap = [
    '#00e5ff', // 0  Title
    '#ff3d5a', // 1  Problem
    '#b8ff00', // 2  Objectives
    '#a855f7', // 3  Architecture
    '#f472b6', // 4  PQC Handshake
    '#3b82f6', // 5  Flink Pipeline
    '#fbbf24', // 6  Spark ML
    '#2dd4bf', // 7  Data Strategy
    '#00e5ff', // 8  Tech Stack
    '#fb923c', // 9  NS-3 Sim
    '#a855f7', // 10 Dashboard
    '#2dd4bf', // 11 Literature
    '#b8ff00', // 12 Roadmap
    '#00e5ff', // 13 Q&A
  ];

  function goTo(index) {
    if (index < 0 || index >= total) return;
    current = index;
    wrapper.style.transform = `translateY(-${current * 100}vh)`;

    // Update sidebar
    sidebar.forEach((item, i) => {
      item.classList.remove('active');
      if (i < current)  item.classList.add('visited');
      if (i === current) {
        item.classList.add('active');
        item.classList.remove('visited');
      }
    });

    // Update counter
    counter.textContent = `${String(current + 1).padStart(2, '0')} / ${String(total).padStart(2, '0')}`;

    // Update glow colour
    if (glow && accentMap[current]) {
      glow.style.background = accentMap[current];
    }

    // Activate animations on current slide
    slides.forEach(s => s.classList.remove('active'));
    slides[current].classList.add('active');

    // Update active sidebar accent colour
    sidebar.forEach(item => {
      if (item.classList.contains('active') && accentMap[current]) {
        item.style.borderLeftColor = accentMap[current];
      }
    });
  }

  // Keyboard navigation
  document.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowRight' || e.key === ' ' || e.key === 'PageDown') {
      e.preventDefault();
      goTo(current + 1);
    } else if (e.key === 'ArrowUp' || e.key === 'ArrowLeft' || e.key === 'PageUp') {
      e.preventDefault();
      goTo(current - 1);
    } else if (e.key === 'Home') {
      e.preventDefault();
      goTo(0);
    } else if (e.key === 'End') {
      e.preventDefault();
      goTo(total - 1);
    }
  });

  // Mouse wheel
  let wheelLock = false;
  document.addEventListener('wheel', (e) => {
    if (wheelLock) return;
    wheelLock = true;
    setTimeout(() => wheelLock = false, 700);
    if (e.deltaY > 0) goTo(current + 1);
    else goTo(current - 1);
  }, { passive: true });

  // Sidebar clicks
  sidebar.forEach((item, i) => {
    item.addEventListener('click', () => goTo(i));
  });

  // Touch support
  let touchStart = 0;
  document.addEventListener('touchstart', (e) => { touchStart = e.changedTouches[0].screenY; });
  document.addEventListener('touchend', (e) => {
    const delta = touchStart - e.changedTouches[0].screenY;
    if (Math.abs(delta) > 50) {
      delta > 0 ? goTo(current + 1) : goTo(current - 1);
    }
  });

  // Initialise
  goTo(0);
})();
