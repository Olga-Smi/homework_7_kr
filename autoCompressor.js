const path = require("path");
const fs = require("fs");
const util = require("util");
const zlib = require("zlib");

const readdir = util.promisify(fs.readdir);
const stat = util.promisify(fs.stat);
const access = util.promisify(fs.access);
const unlink = util.promisify(fs.unlink);

const folderPath = process.argv[2];

const compressQueue = [];
let compressing = false;

async function readFolders(path) {
  try {
    getLogs(`Читаем полученную папку ${path} и работаем с ее файлами`);
    const data = await readdir(path);
    await filesAndFolders(data, path);
  } catch (err) {
    console.error("Ошибка при чтении папки:", err);
  }
}

async function filesAndFolders(dataArr, folderPath) {
  for (const data of dataArr) {
    const dataPath = path.join(folderPath, data);
    console.log(dataPath);

    const dataStat = await stat(dataPath);

    try {
      if (dataStat.isFile()) {
        getLogs(`Получен файл: ${data}`);

        await checkCompressFile(data, dataPath, dataStat);
      } else if (dataStat.isDirectory()) {
        await readFolders(dataPath);
      }
    } catch (err) {
      console.error("Ошибка обрботки файла: ", err);
    }
  }

  getQueue();
}

async function checkCompressFile(data, dataPath, dataStat) {
 
  if (dataPath.endsWith(".gz")) {
    getLogs(`Сжатый файл уже существует`);

    try {
      const compressedDataStat = await stat(dataPath);
      const lastDataUpdate = dataStat.mtime;
      const lastCompressedDataUpdate = compressedDataStat.mtime;

      if (lastDataUpdate > lastCompressedDataUpdate) {
      getLogs(`Сжатая версия устарела, добавляем в очередь на пересжатие`);

      await unlink(compressedDataPath);
      compressQueue.push({ data, dataPath });
    } else {
      getLogs(`Сжатая версия актуальна, пропускаем файл`);
    }
    } catch (err) {console.error(`Ошибка получения статистики сжатого файла: ${err.message}`);}

  } else {
    getLogs(`Сжатая версия не найдена. Добавляем в очередь на создание`);
    compressQueue.push({ data, dataPath });
  }  
}

async function getQueue() {
  if (compressing || compressQueue.length === 0) {
    return;
  }

  compressing = true;

  const { data, dataPath } = compressQueue.shift();
  await getGzip(data, dataPath);

  compressing = false;
  getQueue();
}

async function getGzip(data, dataPath) {
  const gzip = zlib.createGzip();
  const inputStream = fs.createReadStream(dataPath);
  const outputStream = fs.createWriteStream(`${dataPath}.gz`);

  getLogs(`Сжимаем и записываем файл: ${data}`);
  inputStream.pipe(gzip).pipe(outputStream);

  outputStream.on("finish", () => {
    getLogs(`Файл ${data} успешно сжат`);
  });

  inputStream.on("error", (err) => {
    console.error(`Ошибка чтения файла ${dataPath}:`, err);
  });

  outputStream.on("error", (err) => {
    console.error(`Ошибка записи сжатого файла`, err);
  });
}

function getLogs(message) {
  const now = new Date();
  const timestamp =
    now.toISOString().replace("T", " ").replace("Z", "").split(".")[0] +
    "." +
    String(now.getMilliseconds()).padStart(3, "0");
  console.log(`[${timestamp}]: ${message}`);
}

readFolders(folderPath);
