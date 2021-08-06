const _ = require('lodash');
const dot = require('dot-object');
let mongoClient;

function init(db) {
  mongoClient = db;
}

async function aggregateHistory() {
  try{
    const histories = await mongoClient.collection("History").find({}).toArray();
    if(_.isEmpty(histories)) {
      console.log('No history records to aggregate');
      return;
    }

    const aggregatedHistoryArray = _getAggregatedHistory(histories);

    await saveAggregateHistory(aggregatedHistoryArray);

  } catch (e) {
    console.log('Error ', e);
  }
}

async function saveAggregateHistory(aggregatedHistoryArray) {
  try{

    await mongoClient.collection("AggregateHistory").insertMany(aggregatedHistoryArray);
    console.log('Aggregate history records saved successfully');

  } catch (e) {
    console.log('Error saving Aggregate history: ', e);
  }
}

module.exports = {
  aggregateHistory,
  init,
};

function _getAggregatedHistory(histories) {
  histories = histories.sort((a, b) => (
    Object.keys(a.downloadHistory).length > Object.keys(b.downloadHistory).length? -1:
      Object.keys(a.downloadHistory).length < Object.keys(b.downloadHistory).length ?1: 0
  ));
  const aggregatedHistory = histories.reduce((acc, history) => {
    history._id = new Date().getTime().toString();
    const existHistory = acc.find(({uf}) => uf === history.uf);
    if(_.isEmpty(existHistory)) {
      acc.push(history);
      return acc;
    }

    if(!history.downloadHistory) {
      return acc;
    }
    //determine downloadHistory
    _prepareDownloadHistory(history, existHistory);

    const appLaunch = dot.pick('productsUsage.hist.appLaunch', history);
    const existAppLaunch = dot.pick('productsUsage.hist.appLaunch', existHistory)? dot.pick('productsUsage.hist.appLaunch', existHistory): {};
    if(!appLaunch) {
      return acc;
    }
    //determine productsUsage.hist.appLaunch
    _prepareHistAppLaunch(appLaunch, existAppLaunch);

    const usageAppLaunch = dot.pick('productsUsage.appLaunch', history);
    const existUsageAppLaunch = dot.pick('productsUsage.appLaunch', existHistory)? dot.pick('productsUsage.appLaunch', existHistory): {};
    if(!usageAppLaunch) {
      return acc;
    }
    //determine productsUsage.appLaunch
    _prepareProductUsageAppLaunch(appLaunch, existUsageAppLaunch);

    return acc;
  }, []);

  return aggregatedHistory;
}

function _prepareDownloadHistory(history, existHistory) {
  Object.keys(history.downloadHistory).forEach((downloadHistoryKey) => {
    // determine firstDownloadTS
    const firstDownloadTS = history.downloadHistory[downloadHistoryKey].firstDownloadTS;
    const existingFirstDownloadTS = dot.pick(`downloadHistory.${downloadHistoryKey}.firstDownloadTS`, existHistory);
    if(
        firstDownloadTS &&
        existingFirstDownloadTS &&
        new Date(firstDownloadTS) < new Date(existingFirstDownloadTS)
    ) {
      existHistory.downloadHistory[downloadHistoryKey].firstDownloadTS = firstDownloadTS;
    }
    // determine hasDownloaded
    const hasDownloaded = history.downloadHistory[downloadHistoryKey].hasDownloaded;
    const existingHasDownloaded = dot.pick(`downloadHistory.${downloadHistoryKey}.hasDownloaded`, existHistory);
    if(
        !_.isUndefined(hasDownloaded) &&
        !_.isUndefined(existingHasDownloaded)
    ) {
      existHistory.downloadHistory[downloadHistoryKey].hasDownloaded = hasDownloaded || existingHasDownloaded;
    }
    // determine latestDownloadTS
    const latestDownloadTS = history.downloadHistory[downloadHistoryKey].latestDownloadTS;
    const existingLatestDownloadTS = _.get(existHistory, `downloadHistory.${downloadHistoryKey}.latestDownloadTS`, null);
    if(
        latestDownloadTS &&
        existingLatestDownloadTS &&
        new Date(latestDownloadTS) > new Date(existingLatestDownloadTS)
    ) {
      existHistory.downloadHistory[downloadHistoryKey].latestDownloadTS = latestDownloadTS;
    }
    // determine downloadCount
    const downloadCount = history.downloadHistory[downloadHistoryKey].downloadCount;
    const existingDownloadCount = dot.pick(`downloadHistory.${downloadHistoryKey}.downloadCount`, existHistory);
    if(
        downloadCount &&
        existingDownloadCount &&
        downloadCount > existingDownloadCount
    ) {
      existHistory.downloadHistory[downloadHistoryKey].downloadCount = downloadCount;
    }

  });
}

function _prepareHistAppLaunch(appLaunch, existAppLaunch) {
  Object.keys(appLaunch).forEach((appLaunchKey) => {
    if(_.isEmpty(appLaunch[appLaunchKey])) {
      return;
    }

    Object.keys(appLaunch[appLaunchKey]).forEach((innerAppLaunchKey) => {
      const innerAppLaunchValue = appLaunch[appLaunchKey][innerAppLaunchKey];
      const existingInnerAppLaunchValue = existAppLaunch? dot.pick(`${appLaunchKey}.${innerAppLaunchKey}`, existAppLaunch): null;
      if(
          !existingInnerAppLaunchValue &&
          innerAppLaunchValue
      ) {
        if(!existAppLaunch[appLaunchKey]) {
          existAppLaunch[appLaunchKey] = {[innerAppLaunchKey]: innerAppLaunchValue};
        } else {
          existAppLaunch[appLaunchKey][innerAppLaunchKey] = innerAppLaunchValue;
        }
        return;
      }
      if(
          existingInnerAppLaunchValue &&
          innerAppLaunchValue
      ) {
        existAppLaunch[appLaunchKey][innerAppLaunchKey] +=  innerAppLaunchValue;
        return;
      }

    });

  });
}

function _prepareProductUsageAppLaunch(usageAppLaunch, existUsageAppLaunch) {
  Object.keys(usageAppLaunch).forEach((appLaunchKey) => {
    if(_.isEmpty(usageAppLaunch[appLaunchKey])) {
      return;
    }
    // determine firstLaunchTS
    const firstLaunchTS = usageAppLaunch[appLaunchKey].firstLaunchTS;
    const existingFirstLaunchTS = dot.pick(`${appLaunchKey}.firstLaunchTS`, existUsageAppLaunch);
    if(
        firstLaunchTS &&
        existingFirstLaunchTS &&
        new Date(firstLaunchTS) < new Date(existingFirstLaunchTS)
    ) {
      existUsageAppLaunch[appLaunchKey].firstLaunchTS = firstLaunchTS;
    }
    // determine launchCount
    const launchCount =  usageAppLaunch[appLaunchKey].launchCount;
    const existingLaunchCount = dot.pick(`${appLaunchKey}.launchCount`, existUsageAppLaunch);
    if(
        !_.isUndefined(launchCount) &&
        !_.isUndefined(existingLaunchCount)
    ) {
      existUsageAppLaunch[appLaunchKey].launchCount += launchCount;
    }
    // determine latestLaunchTS
    const latestLaunchTS = usageAppLaunch[appLaunchKey].latestLaunchTS;
    const existingLatestLaunchTS = dot.pick(`${appLaunchKey}.latestLaunchTS`, existUsageAppLaunch);
    if(
        latestLaunchTS &&
        existingLatestLaunchTS &&
        new Date(latestLaunchTS) > new Date(existingLatestLaunchTS)
    ) {
      existUsageAppLaunch[appLaunchKey].latestLaunchTS = latestLaunchTS;
    }

  });

}
