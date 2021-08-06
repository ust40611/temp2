const _ = require('lodash');
const dot = require('dot-object');

function aggregateHistory(db) {
  db.collection("History").find({}).toArray((err, documents) => {
    if(err) {
      console.log('Error fetching History: ', err);
      return;
    }

    if(_.isEmpty(documents)) {
      console.log('No history records to aggregate');
      return;
    }
    const aggregatedHistoryArray = getAggregatedHistory(documents);

    db.collection("AggregateHistory").insertMany(aggregatedHistoryArray, (err) => {
      if(err) {
        console.log('Error saving Aggregate history: ', err);
        return;
      }

      console.log('Aggregate history records saved successfully');
      process.exit();
      return;
    });
  });
}


function getAggregatedHistory (documents) {
  documents = documents.sort((a, b) => {
    return Object.keys(a.downloadHistory).length > Object.keys(b.downloadHistory).length? -1:
      Object.keys(a.downloadHistory).length < Object.keys(b.downloadHistory).length ?1: 0;
  })
  const aggregatedHistory = documents.reduce((acc, document) => {
    document._id = new Date().getTime().toString();
    const existHistory = acc.find(({uf}) => uf === document.uf);
    if(_.isEmpty(existHistory)) {
      acc.push(document);
      return acc;
    }

    if(!document.downloadHistory) {
      return acc;
    }

    //determine downloadHistory
    Object.keys(document.downloadHistory).forEach((app) => {
                        
      // determine firstDownloadTS
      const downloadHistoryApp = `downloadHistory.${app}`;
      const existApp = existHistory.downloadHistory[app];
      const firstDownloadTS = dot.pick(`${downloadHistoryApp}.firstDownloadTS`, document);
      const existingFirstDownloadTS = dot.pick(`${downloadHistoryApp}.firstDownloadTS`, existHistory);

      if(
        firstDownloadTS &&
        existingFirstDownloadTS &&
        new Date(firstDownloadTS) < new Date(existingFirstDownloadTS)
      ) {
        existApp.firstDownloadTS = firstDownloadTS;
      }
      // determine hasDownloaded
      const hasDownloaded = dot.pick(`${downloadHistoryApp}.hasDownloaded`, document);
      const existingHasDownloaded = dot.pick(`${downloadHistoryApp}.hasDownloaded`, existHistory);
      if(
        !_.isUndefined(hasDownloaded) &&
        !_.isUndefined(existingHasDownloaded)
      ) {
        existApp.hasDownloaded = hasDownloaded || existingHasDownloaded;
      }
      // determine latestDownloadTS
      const latestDownloadTS = dot.pick(`${downloadHistoryApp}.latestDownloadTS`, document);
      const existingLatestDownloadTS = dot.pick(`${downloadHistoryApp}.latestDownloadTS`, existHistory);
      if(
        latestDownloadTS &&
        existingLatestDownloadTS &&
        new Date(latestDownloadTS) > new Date(existingLatestDownloadTS)
      ) {
        existApp.latestDownloadTS = latestDownloadTS;
      }
      // determine downloadCount
      const downloadCount = dot.pick(`${downloadHistoryApp}.downloadCount`, document);
      const existingDownloadCount = dot.pick(`${downloadHistoryApp}.downloadCount`, existHistory);
      if(
        downloadCount &&
        existingDownloadCount &&
        downloadCount > existingDownloadCount
      ) {
        existApp.downloadCount = downloadCount;
      }

    });

    const histAppLaunch = dot.pick('productsUsage.hist.appLaunch', document);
    const histExistAppLaunch = dot.pick('productsUsage.hist.appLaunch', existHistory)? dot.pick('productsUsage.hist.appLaunch', existHistory): {};
    if(!histAppLaunch) {
      return acc;
    }

    //determine productsUsage.hist.appLaunch
    Object.keys(histAppLaunch).forEach((app) => {
      if(_.isEmpty(histAppLaunch[app])) {
        return;
      }

      Object.keys(histAppLaunch[app]).forEach((launchTime) => {
          // console.log(histAppLaunch[app]) { '2021-05-27': 1, '2021-04-19': 1 }
          // console.log(launchTime) 2021-05-27
        const launchCount = dot.pick(`${app}.${launchTime}`, histAppLaunch)
        const existinglaunchCount = histExistAppLaunch? dot.pick(`${app}.${launchTime}`, histExistAppLaunch): null;

        if(
          !existinglaunchCount &&
          launchCount
        ) {
          if(!histExistAppLaunch[app]) {
            histExistAppLaunch[app] = {[launchTime]: launchCount};
          } else {
            histExistAppLaunch[app][launchTime] = launchCount;
          }
          return;
        }
        if(
          existinglaunchCount &&
          launchCount
        ) {
          histExistAppLaunch[app][launchTime] +=  launchCount;
          return;
        }

      });

    });

    
    const appLaunch = dot.pick('productsUsage.appLaunch', document);
    const existAppLaunch = dot.pick('productsUsage.appLaunch', existHistory)? dot.pick('productsUsage.appLaunch', existHistory): {};
    if(!appLaunch) {
      return acc;
    }

    //determine productsUsage.appLaunch
    Object.keys(appLaunch).forEach((app) => {
      if(_.isEmpty(appLaunch[app])) {
        return;
      }

      // determine firstLaunchTS

      const firstLaunchTS = dot.pick(`${app}.firstLaunchTS`, appLaunch);
      const existingFirstLaunchTS = dot.pick(`${app}.firstLaunchTS`, existAppLaunch);
      const existAppLaunchAPP = existAppLaunch[app];

      if(
        firstLaunchTS &&
        existingFirstLaunchTS &&
        new Date(firstLaunchTS) < new Date(existingFirstLaunchTS)
      ) {
        existAppLaunchAPP.firstLaunchTS = firstLaunchTS;
      }

      // determine launchCount
      const launchCount =  dot.pick(`${app}.launchCount`, appLaunch);
      const existingLaunchCount = dot.pick(`${app}.launchCount`, existAppLaunch);
      if(
        !_.isUndefined(launchCount) &&
        !_.isUndefined(existingLaunchCount)
      ) {
        existAppLaunchAPP.launchCount += launchCount;
      }
      // determine latestLaunchTS
      const latestLaunchTS = dot.pick(`${app}.latestLaunchTS`, appLaunch);
      const existingLatestLaunchTS = dot.pick(`${app}.latestLaunchTS`, existAppLaunch);
      if(
        latestLaunchTS &&
        existingLatestLaunchTS &&
        new Date(latestLaunchTS) > new Date(existingLatestLaunchTS)
      ) {
        existAppLaunchAPP.latestLaunchTS = latestLaunchTS;
      }

    });

    return acc;
  }, []);

  return aggregatedHistory;
}

module.exports = {
  aggregateHistory,
};
