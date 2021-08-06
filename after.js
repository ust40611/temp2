const _ = require("lodash");
const dot = require("dot-object");

function updateDownloadHistory(document, existDocument) {
  Object.keys(document.downloadHistory).forEach((app) => {
    // determine firstDownloadTS
    const downloadHistoryApp = `downloadHistory.${app}`;
    const existApp = existDocument.downloadHistory[app];
    const firstDownloadTS = dot.pick(`${downloadHistoryApp}.firstDownloadTS`, document);
    const existingFirstDownloadTS = dot.pick(`${downloadHistoryApp}.firstDownloadTS`, existDocument);
    if (
      firstDownloadTS &&
      existingFirstDownloadTS &&
      new Date(firstDownloadTS) < new Date(existingFirstDownloadTS)
    ) {
      existApp.firstDownloadTS = firstDownloadTS;
    }
    // determine hasDownloaded
    const hasDownloaded = dot.pick(`${downloadHistoryApp}.hasDownloaded`, document);
    const existingHasDownloaded = dot.pick(`${downloadHistoryApp}.hasDownloaded`, existDocument);
    if (
      !_.isUndefined(hasDownloaded) && !_.isUndefined(existingHasDownloaded)
    ) {
      existApp.hasDownloaded = hasDownloaded || existingHasDownloaded;
    }
    // determine latestDownloadTS
    const latestDownloadTS = dot.pick(`${downloadHistoryApp}.latestDownloadTS`, document);
    const existingLatestDownloadTS = dot.pick(`${downloadHistoryApp}.latestDownloadTS`, existDocument);
    if (
      latestDownloadTS &&
      existingLatestDownloadTS &&
      new Date(latestDownloadTS) > new Date(existingLatestDownloadTS)
    ) {
      existApp.latestDownloadTS = latestDownloadTS;
    }
    // determine downloadCount
    const downloadCount = dot.pick(`${downloadHistoryApp}.downloadCount`, document);
    const existingDownloadCount = dot.pick(`${downloadHistoryApp}.downloadCount`, existDocument);
    if (
      downloadCount && existingDownloadCount && downloadCount > existingDownloadCount
    ) {
      existApp.downloadCount = downloadCount;
    }
  });
}

function updateHistApplaunch(document, existDocument, acc) {
  const histAppLaunch = dot.pick("productsUsage.hist.appLaunch", document);
  const histExistAppLaunch = dot.pick("productsUsage.hist.appLaunch", existDocument)
    ? dot.pick("productsUsage.hist.appLaunch", existDocument) : {};
  if (!histAppLaunch) return acc;
  //determine productsUsage.hist.appLaunch
  Object.keys(histAppLaunch).forEach((app) => {
    if (_.isEmpty(histAppLaunch[app])) return;
    Object.keys(histAppLaunch[app]).forEach((launchTime) => {
      const launchCount = dot.pick(`${app}.${launchTime}`, histAppLaunch);
      const existinglaunchCount = histExistAppLaunch ? dot.pick(`${app}.${launchTime}`, histExistAppLaunch) : null;
      if (!existinglaunchCount && launchCount) {
        if (!histExistAppLaunch[app]) {
          histExistAppLaunch[app] = { [launchTime]: launchCount };
        } else {
          histExistAppLaunch[app][launchTime] = launchCount;
        }
        return;
      }
      if (existinglaunchCount && launchCount) {
        histExistAppLaunch[app][launchTime] += launchCount;
        return;
      }
    });
  });
}

function updateApplaunch(document, existDocument, acc) {
  const appLaunch = dot.pick("productsUsage.appLaunch", document);
  const existAppLaunch = dot.pick("productsUsage.appLaunch", existDocument)
    ? dot.pick("productsUsage.appLaunch", existDocument) : {};
  if (!appLaunch) return acc;

  //determine productsUsage.appLaunch
  Object.keys(appLaunch).forEach((app) => {
    if (_.isEmpty(appLaunch[app])) return;

    // determine firstLaunchTS
    const firstLaunchTS = dot.pick(`${app}.firstLaunchTS`, appLaunch);
    const existingFirstLaunchTS = dot.pick(`${app}.firstLaunchTS`, existAppLaunch);
    const existAppLaunchAPP = existAppLaunch[app];
    if (
      firstLaunchTS && 
      existingFirstLaunchTS &&
      new Date(firstLaunchTS) < new Date(existingFirstLaunchTS)
    ) {
      existAppLaunchAPP.firstLaunchTS = firstLaunchTS;
    }
    // determine launchCount
    const launchCount = dot.pick(`${app}.launchCount`, appLaunch);
    const existingLaunchCount = dot.pick(`${app}.launchCount`, existAppLaunch);
    if (!_.isUndefined(launchCount) && !_.isUndefined(existingLaunchCount)) {
      existAppLaunchAPP.launchCount += launchCount;
    }
    // determine latestLaunchTS
    const latestLaunchTS = dot.pick(`${app}.latestLaunchTS`, appLaunch);
    const existingLatestLaunchTS = dot.pick(`${app}.latestLaunchTS`, existAppLaunch);
    if (
      latestLaunchTS &&
      existingLatestLaunchTS &&
      new Date(latestLaunchTS) > new Date(existingLatestLaunchTS)
    ) {
      existAppLaunchAPP.latestLaunchTS = latestLaunchTS;
    }
  });
}

function aggregateDocuments(documents) {
  documents = documents.sort((a, b) => {
    return Object.keys(a.downloadHistory).length > Object.keys(b.downloadHistory).length? -1:
      Object.keys(a.downloadHistory).length < Object.keys(b.downloadHistory).length ?1: 0
  });
  const aggregatedDocument = documents.reduce((acc, document) => {
    document._id = new Date().getTime().toString(); //set the _id to aggregated ts in milliseconds
    const existDocument = acc.find(({ uf }) => uf === document.uf);
    if (_.isEmpty(existDocument)) {
      acc.push(document);
      return acc;
    }
    if (!document.downloadHistory) return acc;
    updateDownloadHistory(document, existDocument);
    updateHistApplaunch(document, existDocument, acc);
    updateApplaunch(document, existDocument, acc);
    return acc;
  }, []);
  return aggregatedDocument;
}

async function run(db) {
  try {
    const documents = await db.collection("History").find({}).toArray();
    if (_.isEmpty(documents)){
        console.log("No history records to aggregate");
        return;
    }
    const aggregatedHistoryArray = aggregateDocuments(documents);
    const result = await db.collection("AggregateHistory").insertMany(aggregatedHistoryArray);
    console.log("Aggregate history records saved successfully");
    process.exit();
  } catch (err) {
    console.log(err.stack);
  }
}

module.exports = {
  run,
};
