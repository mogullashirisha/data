chrome.webRequest.onAuthRequired.addListener(
  function(details, callbackFn) {
      console.log("onAuthRequired!", details, callbackFn);
      callbackFn({
        'authCredentials': {username: "t7.devasishmahato@gmail.com", password: "123password$$"}
      });
  },
  {urls: ["<all_urls>"]},
  ['asyncBlocking']
);