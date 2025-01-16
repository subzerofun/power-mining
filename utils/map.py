from flask import Blueprint, render_template

# Create blueprint for map functionality
map_bp = Blueprint('map', __name__)

@map_bp.route('/map')
def show_map():
    """Render the map page."""
    return render_template('map.html')
