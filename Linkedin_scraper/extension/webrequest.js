chrome.webRequest.onAuthRequired.addListener(
function handler(details){
 return {'authCredentials': {username: "t7.devasishmahato@gmail.com", password: "123password$$"}};
},
{urls:["<all_urls>"]},
['blocking']);