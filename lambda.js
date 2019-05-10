const https = require('https');
const readline = require('readline');

const AWS = require('aws-sdk');
const fs = require('fs');

AWS.config.update({
    secretAccessKey: process.env.SECRET_ACCESS_KEY,
    accessKeyId: process.env.ACCESS_KEY_ID,
    region: 'us-east-1'
});
const S3 = new AWS.S3();
const S3_BUCKET = 'bucket';

const inputPath = '/tmp/input';
const outputPath = '/tmp/output';
const inputFileName = 'input';
const inputPathFileName = `${inputPath}/${inputFileName}.csv`;

exports.handler = async (event) => {

	if (event.httpMethod == 'POST') {

		try {

			let data = JSON.parse(event.body);

			let { url } = data;

			downloadFile(url);

			return sendRes(200, 'processing file');

	    } catch (e) {
	     	handleError(e);
	    	return sendRes(500, 'something went wrong');
	    }
	}
}

function downloadFile(url){

	try {

		if (!fs.existsSync(inputPath)) {
			fs.mkdirSync(inputPath);
		}

		if (!fs.existsSync(outputPath)) {
			fs.mkdirSync(outputPath);
	 	}

	} catch(err) {
	  	console.error(err);
	}

	console.log(`Downloading file from: ${url}`);

	const file = fs.createWriteStream(inputPathFileName);
	const request = https.get(url, response =>{

	 	const { statusCode } = response;

	 	if (statusCode !== 200) {
	 		removeFileSync(inputPathFileName);
	 		handleError(Error(`${statusCode} status code`));
	 	}

		response.pipe(file);

		file.on('finish', () =>{

      		file.close();
			console.log(`File downloaded. Beginning to process it`);
      		processFile();
    	});
  	})

  	request.on('error', err =>{

	 	removeFileSync(inputPathFileName);
    	handleError(err);

  	});
}

function processFile(){

	const readStream = fs.createReadStream(inputPathFileName);
	const readLine = readline.createInterface({
		input: readStream,
	  	creadLinefDelay: Infinity
	});
	const writeStreamOptions = {
		flags: 'a'
	};


	readLine.on('line', line => {

		process.stdout.write('.');

		let {
			fileName,
			fileContent
		} = getFileNameAndContent(line);


	  	let writeStream = fs.createWriteStream(`${outputPath}/${fileName}.csv`, writeStreamOptions);

		writeStream.write(fileContent);

		writeStream.on('error', e => {
			console.log(e);
		})

		writeStream.end();
	});

	readLine.on('close', () =>{
		console.log('\nInput File read');
		uploadToS3();
	})
}

function handleError(err){
	console.log(err);
}

function removeFileSync(filename){

	try {
	  fs.unlinkSync(filename);
	} catch(err) {
	  console.log(err);
	}
}

function getFileNameAndContent(line){

	let l, fileName = '', fileContent;

	for (l = 0; l < line.length; l++) {

		if(line[l] === ','){
			break;
		}

		fileName += line[l];
	}

	fileContent = line.substring(l + 1, line.length) + '\n';

	return {
		fileName,
		fileContent
	}
}

async function uploadToS3(){

	try{

		let urls = [];

		fs.readdirSync(outputPath).forEach(async file => {

			console.log(file);
		 	let url = await uploadFile(file);
		 	urls.push(url);
			console.log(`removing local copy of ${file}`);
	 		removeFileSync(`${outputPath}/${file}`);
		})

	}catch(error){
		console.log(error);
	}
}

function uploadFile(fileName){
	return new Promise((resolve, reject) => {

		const readStream = fs.createReadStream(fileName);

		console.log(`uploading to s3: ${fileName}`);

		let params = {
		    Bucket: S3_BUCKET,
		    Key: `output/${fileName}.csv`,
		    Body: readStream
		};

		S3.upload(params, (err, data) =>{

			if(err){
				reject(err);
			}else{
				console.log(`uploaded to s3: ${fileName} with ${data.Location}`);
				resolve(data.Location);
			}

		});
	})
}