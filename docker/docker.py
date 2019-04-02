from flask import Flask, render_template, Markup
from svg import SVGObject
app = Flask(__name__)

@app.route("/")
def hello():
    svg = SVGObject()
    print(svg.return_scroll_script)
    return render_template('main.html', title='Platform Docker', 
    	svg_index=Markup(svg.return_index_svg()), 
    	svg_main=Markup(svg.return_main_svg()),
    	scroll_js=Markup(svg.return_scroll_script()))
    

if __name__ == '__main__':
    app.run(debug=True)