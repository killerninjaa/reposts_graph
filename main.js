import Graph from "graphology";
import Sigma from "sigma";
import FA2Layout from "graphology-layout-forceatlas2/worker";

const BRIGHT_PASTEL_COLORS = [
  "#FF9999", // розовый (насыщенный)
  "#66B3FF", // голубой
  "#99FF99", // зелёный
  "#FFCC99", // оранжевый
  "#FF99CC", // фиолетово-розовый
  "#99CCFF", // светло-голубой
  "#FFD700", // золотой
  "#CC99FF", // лиловый
  "#99FFCC", // мятный
  "#FF9966"  // коралловый
];

function getColorFromString(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  const index = Math.abs(hash) % BRIGHT_PASTEL_COLORS.length;
  return BRIGHT_PASTEL_COLORS[index];
}

// Функция загрузки данных
async function loadGraphData() {
  const response = await fetch("forwards_graph_2_level.json");
  return await response.json();
}

// Функция создания графа с фильтрацией по категориям
async function createFilteredGraph(categories) {
  const graph = new Graph();
  const data = await loadGraphData();

  // Словари для накопления данных по нодам
  const nodeInfo = {};      // хранит объединённые source_info + target_info
  const incomingReposts = {}; // хранит входящие репосты для каждой ноды (если она target)
  const outgoingCount = {};   // хранит количество исходящих репостов (если она source)

  // Инициализируем структуры
  data.forEach(item => {
    const source = item.source;
    const target = item.target;
    const category = item.source_info?.category;

    // Проверяем, соответствует ли категория одной из выбранных
    if (categories.includes(category)) {
      // --- Инициализируем или обновляем информацию о source ноде ---
      if (source) {
        if (!nodeInfo[source]) {
          nodeInfo[source] = {};
        }
        // Обогащаем из source_info
        Object.assign(nodeInfo[source], {
          label: item.source_info.title || source,
          url: item.source_info.link,
          participants: item.source_info.participants_cnt,
          peer_type: item.source_info.peer_type,
          category: item.source_info.category,
          about: item.source_info.about,
          rkn_verification: item.source_info.rkn_verification,
          country: item.source_info.country,
          language: item.source_info.language,
          red_label: item.source_info.red_label,
          black_label: item.source_info.black_label
        });

        // Инициализируем счётчики
        if (!outgoingCount[source]) outgoingCount[source] = 0;
        if (!incomingReposts[source]) incomingReposts[source] = [];
      }

      // --- Инициализируем или обновляем информацию о target ноде ---
      if (target) {
        if (!nodeInfo[target]) {
          nodeInfo[target] = {};
        }
        // Обогащаем из target_info
        Object.assign(nodeInfo[target], {
          label: item.target_info.title || target,
          url: item.target_info.link,
          participants: item.target_info.participants_cnt,
          about: item.target_info.about, // может быть только в target_info
        });

        // Инициализируем счётчики
        if (!outgoingCount[target]) outgoingCount[target] = 0;
        if (!incomingReposts[target]) incomingReposts[target] = [];
      }

      // --- Добавляем репост в список входящих для target ---
      if (source && target && source !== target) {
        incomingReposts[target].push({
          from: source,
          post: item.post
        });
        outgoingCount[source] += 1;
      }
    }
  });

  // --- Создаём ноды с полной информацией ---
  Object.keys(nodeInfo).forEach(nodeId => {
    const info = nodeInfo[nodeId];
    const inCount = incomingReposts[nodeId].length;
    const outCount = outgoingCount[nodeId];

    // Логарифмический размер ноды: зависит и от входящих, и от исходящих репостов
    const nodeSize = Math.log(outCount + 1) * 3 + 3;

    graph.addNode(nodeId, {
      ...info, // все поля из JSON
      size: Math.min(1000, nodeSize),
      color: getColorFromString(nodeId),
      x: Math.random(),
      y: Math.random(),
      incomingReposts: incomingReposts[nodeId], // ✅ список входящих репостов
      outgoingCount: outCount                   // ✅ счётчик исходящих
    });
  });

  // --- Создаём рёбра ---
  const edgeMap = {}; // для агрегации постов

  data.forEach(item => {
    const { source, target } = item;
    if (!source || !target || source === target || !categories.includes(item.source_info?.category)) return;

    const edgeKey = `${source}->${target}`;

    if (!edgeMap[edgeKey]) {
      edgeMap[edgeKey] = {
        source,
        target,
        posts: []
      };
    }

    edgeMap[edgeKey].posts.push(item.post);
  });

  // Добавляем рёбра в граф
  Object.values(edgeMap).forEach(edgeData => {
    const { source, target, posts } = edgeData;
    const edgeKey = `${source}->${target}`;

    graph.addEdgeWithKey(edgeKey, source, target, {
      color: "rgba(136, 136, 136, 0.8)",
      size: 1,
      directed: true,
      type: "arrow",
      posts: posts // все посты по этому ребру
    });
  });

  // Возвращаем граф без применения ForceAtlas2
  return graph;
}

// Функция получения уникальных категорий из данных
async function getUniqueCategories() {
  const data = await loadGraphData();
  const categoriesSet = new Set();

  data.forEach(item => {
    if (item.source_info?.category) {
      categoriesSet.add(item.source_info.category);
    }
  });

  return Array.from(categoriesSet);
}

// Функция для инициализации выбора категорий
async function initCategorySelection() {
  const categoryCheckboxes = document.getElementById("category-checkboxes");
  const uniqueCategories = await getUniqueCategories();

  uniqueCategories.forEach(category => {
    const checkboxDiv = document.createElement("div");
    checkboxDiv.className = "category-checkbox";
    checkboxDiv.textContent = category;
    checkboxDiv.addEventListener("click", () => {
      checkboxDiv.classList.toggle("selected");
    });
    categoryCheckboxes.appendChild(checkboxDiv);
  });
}

// Функция для отображения графа после фильтрации
async function renderFilteredGraph() {
  const categoryCheckboxes = document.querySelectorAll(".category-checkbox");
  const selectedCategories = Array.from(categoryCheckboxes)
    .filter(checkbox => checkbox.classList.contains("selected"))
    .map(checkbox => checkbox.textContent);

  if (selectedCategories.length === 0) {
    alert("Пожалуйста, выберите хотя бы одну категорию.");
    return;
  }

  const container = document.getElementById("graph-container");
  const graph = await createFilteredGraph(selectedCategories);

  // Проверяем, содержит ли граф узлы и рёбра
  if (graph.order === 0 || graph.size === 0) {
    alert("Нет данных для выбранных категорий.");
    return;
  }

  // Скрываем блок выбора категорий
  document.getElementById("category-selection").style.display = "none";

  // Показываем кнопку сброса
  document.getElementById("reset-button").style.display = "block";

  // Показываем контейнер графа
  container.style.visibility = "visible";

  // Очищаем предыдущий рендерер, если он существует
  if (window.currentRenderer) {
    window.currentRenderer.kill(); // Уничтожаем предыдущий рендерер
    delete window.currentRenderer; // Удаляем ссылку на предыдущий рендерер
  }

  // Инициализируем рендерер
  const renderer = new Sigma(graph, container, { renderLabels: false });
  window.currentRenderer = renderer; // Сохраняем ссылку на текущий рендерер

  // Определяем время работы ForceAtlas2 в зависимости от количества узлов
  const nodeCount = graph.order;
  let duration = 90000; // 30 секунд по умолчанию для больших графов
  let settings = {
    iterations: 1,
    scalingRatio: 1,
    strongGravityMode: true,
    gravity: 50000,
    adjustSizes: true,
    linLogMode: true,
    edgeWeightInfluence: 1000
  }
  if (nodeCount <= 500) {
    duration = 3000; // 5 секунд для графов с 10 и менее узлами
    settings.scalingRatio = 10
    settings.gravity = 10
  } else if (nodeCount <= 1000) {
    duration = 5000; // 5 секунд для графов с 10 и менее узлами
    settings.scalingRatio = 10
  } else if (nodeCount <= 1500) {
    duration = 10000; // 5 секунд для графов с 10 и менее узлами
    settings.scalingRatio = 1
  } else if (nodeCount <= 2000) {
    duration = 10000; // 10 секунд для графов с 11-20 узлами
    settings.scalingRatio = 100
  } else if (nodeCount <= 3000) {
    duration = 10000; // 10 секунд для графов с 11-20 узлами
    settings.scalingRatio = 1
  } else if (nodeCount <= 5000) {
    duration = 15000; // 15 секунд для графов с 21-50 узлами
  }

  const layout = new FA2Layout(graph, { settings });
  layout.start();

  // Остановить через вычисленное время
  setTimeout(() => {
    layout.stop();
    console.log(`ForceAtlas2 остановлен после ${duration / 1000} секунд работы`);
  }, duration);

  // --- Элементы инфо-окна ---
  const infoBox = document.getElementById("info-box");
  const closeButton = document.getElementById("close-info-box");
  const channelName = document.getElementById("channel-name");
  const channelLink = document.getElementById("channel-link");
  const participants = document.getElementById("participants");
  const repostsContainer = document.getElementById("reposts-container");
  const repostsList = document.getElementById("reposts-list");

  const hideInfoBox = () => {
    infoBox.classList.remove("visible");
  };

  // --- Логика событий ---
  renderer.on("clickNode", ({ node }) => {
    const nodeAttrs = graph.getNodeAttributes(node);
    channelName.textContent = nodeAttrs.label || node;
    channelLink.href = "https://" + (nodeAttrs.url || "");
    channelLink.textContent = nodeAttrs.url || "-";
    participants.textContent = nodeAttrs.participants ? nodeAttrs.participants.toLocaleString() : "-";
    const peerTypeEl = document.getElementById("peer-type");
    const categoryEl = document.getElementById("category");
    const countryEl = document.getElementById("country");
    const languageEl = document.getElementById("language");
    const rknStatusEl = document.getElementById("rkn-status");
    const redLabelEl = document.getElementById("red-label");
    const blackLabelEl = document.getElementById("black-label");
    const outgoingCountEl = document.getElementById("outgoing-count");
    const aboutTextEl = document.getElementById("about-text");

    // Заполняем все поля с fallback на "-"
    peerTypeEl.textContent = nodeAttrs.peer_type || "-";
    categoryEl.textContent = nodeAttrs.category || "-";
    countryEl.textContent = nodeAttrs.country || "-";
    languageEl.textContent = nodeAttrs.language || "-";
    rknStatusEl.textContent = nodeAttrs.rkn_verification || "-";
    redLabelEl.textContent = nodeAttrs.red_label !== null ? (nodeAttrs.red_label ? "Да" : "Нет") : "-";
    blackLabelEl.textContent = nodeAttrs.black_label !== null ? (nodeAttrs.black_label ? "Да" : "Нет") : "-";
    outgoingCountEl.textContent = nodeAttrs.outgoingCount || "0";
    aboutTextEl.textContent = nodeAttrs.about || "Описание отсутствует";

    // --- Отображаем входящие репосты ---
    repostsList.innerHTML = "";
    const reposts = nodeAttrs.incomingReposts || [];

    if (reposts.length > 0) {
      repostsContainer.style.display = "block";

      reposts.forEach(({ from, post }) => {
        const fromNodeAttrs = graph.getNodeAttributes(from) || {};
        const postDate = new Date(post.date * 1000).toLocaleString();

        const postElement = document.createElement("div");
        postElement.className = "repost-item";
        postElement.innerHTML = `
          <p><strong>Откуда:</strong> ${fromNodeAttrs.label || from}</p>
          <p><strong>Дата:</strong> ${postDate}</p>
          <p><strong>Просмотров (оригинальная запись):</strong> ${post.views ? post.views.toLocaleString() : "-"}</p>
          <p><a href="${post.link}" target="_blank">Ссылка на пост</a></p>`;
        repostsList.appendChild(postElement);
      });
    } else {
      repostsContainer.style.display = "none";
    }

    // --- Позиционирование окна ---
    infoBox.style.opacity = "0";
    infoBox.classList.add("visible");

    const boxRect = infoBox.getBoundingClientRect();
    const top = (window.innerHeight - boxRect.height) / 2;
    const left = (window.innerWidth - boxRect.width) / 2;

    infoBox.style.top = `${top}px`;
    infoBox.style.left = `${left}px`;
    infoBox.style.opacity = "1";
  });

  // Закрытие окна
  closeButton.addEventListener("click", hideInfoBox);
  renderer.on("clickStage", hideInfoBox);
}

// Функция сброса к первому экрану с выбором категории
function resetToCategorySelection() {
  // Скрываем контейнер графа
  document.getElementById("graph-container").style.visibility = "hidden";

  // Скрываем кнопку сброса
  document.getElementById("reset-button").style.display = "none";

  // Показываем блок выбора категорий
  document.getElementById("category-selection").style.display = "flex";

  // Очищаем выбранные категории
  const categoryCheckboxes = document.querySelectorAll(".category-checkbox");
  categoryCheckboxes.forEach(checkbox => {
    checkbox.classList.remove("selected");
  });

  // Очищаем предыдущий рендерер, если он существует
  if (window.currentRenderer) {
    window.currentRenderer.kill(); // Уничтожаем предыдущий рендерер
    delete window.currentRenderer; // Удаляем ссылку на предыдущий рендерер
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  await initCategorySelection();

  const confirmButton = document.getElementById("confirm-button");
  confirmButton.addEventListener("click", renderFilteredGraph);

  const resetButton = document.getElementById("reset-button");
  resetButton.addEventListener("click", resetToCategorySelection);
});