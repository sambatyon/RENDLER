var page = require('webpage').create(),
    system = require('system'),
    address, output, size;

if (system.args.length < 3) {
  console.log('Usage: render.js <url> <destination>');
  phantom.exit(1);
}

address = system.args[1];
destination = system.args[2];

console.log('Rendering ' + address + ' to ' + destination);

page.viewportSize = {
    width: 1024,
    height: 768
};

page.onResourceError = function(resourceError) {
    page.reason = resourceError.errorString;
    page.reason_url = resourceError.url;
};

page.open(address, function(status) {
    if (status === 'success') {
        page.render(destination);
        phantom.exit(0);
    } else {
        errstr = "Error opening url '" + page.reason_url + "': " + page.reason;
        system.stderr.writeLine(errstr);
        console.log(errstr);
        phantom.exit(1);
    }
});
