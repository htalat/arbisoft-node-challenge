const readline = require('readline');

const fs = require('graceful-fs'); 

const inputFile = process.argv[2];

if(typeof inputFile === 'undefined'){
	console.log('input file not provided');
	process.exit(1);
}

if (!fs.existsSync(inputFile)) {
	console.log('input file does not exist');
	process.exit(1);
}

if (!fs.existsSync('output')) {
	fs.mkdirSync('output')
}

const readStream = fs.createReadStream(inputFile);
const readLine = readline.createInterface({
	input: readStream,
  	creadLinefDelay: Infinity
});
const writeStreamOptions = {
	flags: 'a'
};

readLine.on('line', line => {

	let {
		fileName,
		fileContent
	} = getFileNameAndContent(line);


  	let writeStream = fs.createWriteStream(`output/${fileName}.csv`, writeStreamOptions);

	writeStream.write(fileContent);

	writeStream.on('error', e => {
		console.log(e);
	})

	writeStream.end();
});

readLine.on('close', () => {

	console.log('\nInput File read');

})

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
