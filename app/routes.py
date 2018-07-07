from flask import Blueprint, render_template
from .feeds import Feed, db

#------------------------------------------------------------------------------#
# Controllers
#------------------------------------------------------------------------------#

bp = Blueprint('centipede', __name__, template_folder='templates')

blueprints = [bp]

@bp.record_once
def init_db(state):
    db.init_app(state.app)
    db.app = state.app
    with state.app.app_context():
        db.create_all()

@bp.route("/")
def index():
    return render_template("feeds.html", feeds=Feed.feeds)

@bp.route("/feed/<id>")
def feed(id):
    feed = Feed.feeds[id]
    feed.crawl()
    return render_template("feed.atom", feed=feed, entries=feed.entries)

# Error Handlers

@bp.app_errorhandler(500)
def internal_error(error):
    models.db_session.rollback()
    return render_template('500.html'), 500

@bp.route('/<path:invalid_path>')
def internal_error(invalid_path):
    return render_template('404.html'), 404
