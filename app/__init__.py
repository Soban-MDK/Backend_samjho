# app/__init__.py
from flask import Flask
from flask_caching import Cache

cache = Cache(config={
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_HOST': 'localhost',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 0,
    'CACHE_DEFAULT_TIMEOUT': 3600
})

def create_app():
    app = Flask(__name__)
    cache.init_app(app)
    
    from app.routes import report_routes
    app.register_blueprint(report_routes.bp)
    
    return app