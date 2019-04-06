from flask import Flask, render_template, Markup
from svg import SVGObject
app = Flask(__name__, static_url_path='/static')

@app.route("/")
def hello():
    svg = SVGObject()

    return render_template('main.html', title='Platform Docker', 
    	svg_index=Markup(svg.return_index_svg()),  # The SVG containing the platform row index
    	svg_main=Markup(svg.return_main_svg()),  # The SVG containing the main docker background.
    	svg_scroll_script=Markup(svg.return_scroll_left()),
    	auto_scroll_script=Markup(svg.return_auto_scroll_js()))
    

if __name__ == '__main__':
    app.run(debug=True)