module.exports = {
    assetsDir: 'static', // For simple configuration of static files in Flask (the "static_folder='client/dist/static'" part in app.py)
    devServer: {
        proxy: {
            '/api': {
                target: 'http://localhost:5000'
            },
            '/report' : {
                target: 'http://localhost:5000'
            }
        }
    }
};