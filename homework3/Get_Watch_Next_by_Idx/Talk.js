const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
	    _id: String,
	    main_speaker: String,
	    title: String,
	    details: String,
	    url: String,
	    num_views: String,
	    tags: String,
	    wnext: [String],
	    wnurl: [String],
	    data: Date
}, { collection: 'tedz_data' });

module.exports = mongoose.model('talk', talk_schema);
