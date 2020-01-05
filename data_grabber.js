const rp = require('request-promise')
const fs = require('fs')
const rll = require('read-last-lines')

const url = 'https://api.binance.com'

var interval = 106

const pair = process.argv[2] || 'BTCUSDT'
const multiplier = 60

startTime = 1525129200000 // may 01 2018

const filename = `history_${pair}`

grab = () => rp.get(
    { url: `${url}/api/v1/aggTrades?symbol=BTCUSDT&startTime=${startTime}&endTime=${startTime+60000*multiplier}`}
).then(body => {
    const result = JSON.parse(body)
    if(result.length > 0){
        const appendingText = result.map(e => `${JSON.stringify(e)}\n`).join('')
        fs.appendFileSync(filename, appendingText, {encoding:'utf-8'})
    }
    startTime += 60000*multiplier
    console.log(new Date(result[result.length-1].T))
    if (new Date().getTime() > startTime) {
        setTimeout(grab, interval)
    }
})

rll.read(filename, 1)
    .then((lines) => {
        const lastTrade = JSON.parse(lines)
        startTime = lastTrade.T + 1
        grab()
    }).catch(grab)
