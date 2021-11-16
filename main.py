import logic
from flask import Flask, render_template, redirect, url_for, request
from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired

app = Flask(__name__)
app.config['SECRET_KEY'] = 'you-will-never-guess'


class MyForm(FlaskForm):
    code = StringField('code', validators=[DataRequired()])


@app.route("/index")
def index():
    code = request.args.get('code')
    # code = "RU000A0JRVU3"
    link = f"http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/{code}.json?from=2021-11-11&till=2021-11-12"
    k = logic.MOEXDecoder(link).data_tuple()

    return render_template("index.html",
                           ISIN=k[0][2],
                           name=k[0][3]
                           )


@app.route('/search', methods=['GET', 'POST'])
def submit():
    form = MyForm()
    if form.validate_on_submit():
        return redirect(url_for('index', code=form.code.data))
    return render_template('search.html', form=form)


if __name__ == '__main__':
    app.run(port=5000, debug=True)

    # https://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/RU000A0JRVU3.json?from=2021-11-11&till=2021-11-12
