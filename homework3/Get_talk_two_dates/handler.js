const connect_to_db = require('./db'); //va a caricare db.js

// GET BY TALK HANDLER

const talk = require('./Talk'); //tiro su i DB

module.exports.get_two_dates = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body) //prendo il body e lo parso in un json quindi string ->json, lo faccio perchè così posso settare i parametri
    }
    // set default
    if(!body.data1) { 
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'BODY PROBLEM: Could not fetch the talks. data1 is null.'
        })
    }
    
    if(!body.data2) { 
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'BODY PROBLEM: Could not fetch the talks. data2 is null.'
        })
    }
    
    
    //facio una paginazione, in modo da non avere ogni volta tutti i talk ma solo 10 alla volta
    if (!body.doc_per_page) {
        body.doc_per_page = 10
    }
    if (!body.page) {
        body.page = 1 //valore di default
    }

    connect_to_db().then(() => { 
        console.log('=> get talks between two dates');
        talk.find({ data : {$gt : body.data1, $lt : body.data2}}).select('main_speaker title details data').sort({'data': 1}) 
            .skip((body.doc_per_page * body.page) - body.doc_per_page)
            .limit(body.doc_per_page)
            .then(talks => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(talks)
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the talks.'
                })
            );
    });
};
