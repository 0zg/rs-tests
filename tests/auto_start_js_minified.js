//Javascript wont work unminified and with qoutes ("), minfy javascript and replace qoutes (" >> ')
$(function(){var e='Species,Identifiant\r\n';e+='Species A,320439\r\n',e+='Species B,349450\r\n',e+='Species C,43435904\r\n',e+='Species D,320440\r\n',e+='Species E,349451\r\n',e+='Species F,43435905\r\n';var t=[];t=function e(t){var s=t.split(/\r\n|\n/);console.log('run processData()');var o=[];for(s.shift().split(',');s.length>0;){var r,n={};r=s.shift().split(','),n.label=r[0],n.value=r[1],o.push(n)}return o}(e);var s='';$('#species').autocomplete({minLength:3,delay:600,source:t,focus:function(e,t){return $('#species').val(t.item.label),!1}}).on('autocompletesearch',function(e,o){var r=$('#species').val();if(s!=r){console.log(r);var n='https://sv2000136.frd.shsdir.nl/ReportServer_SSRS?/_Tests/self/test_autocomplete_v02_getcsv&rs:format=csv&term='+r,n='http://localhost:1180/auto_getcsv.xlsx',n='http://localhost/report?/test/auto_getcsv&rs:Format=EXCELOPENXML&term='+r;console.log(n),console.log('downloading.');var a=[],l=new XMLHttpRequest;l.open('GET',n,!0),l.responseType='arraybuffer',l.onload=function(e){var t=new Uint8Array(l.response),s=XLSX.read(t,{type:'array'});console.log('downloading..process.xlsheet:'+s.SheetNames[0]);var o=s.Sheets[s.SheetNames[0]],r=XLSX.utils.sheet_to_json(o,{header:1});if(r.length>0)for(var n=0;n<r.length;n++)if(0==n);else{var i={};i.label=r[n][0],i.value=r[n][1],a.push(i)}console.log('downloading...done')},l.onloadend=function(e){t=a,$('#species').autocomplete('option','source',t),$('#species').autocomplete('search',r),s=r,console.log('dropdown_autolist')},l.send()}})});