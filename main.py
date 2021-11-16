import logic
from flask import Flask, render_template, redirect, url_for, request
from flask_wtf import FlaskForm
from wtforms import StringField, DateField
from wtforms.validators import DataRequired

app = Flask(__name__)
app.config['SECRET_KEY'] = 'you-will-never-guess'


class MyForm(FlaskForm):
    code = StringField('ISIN облигации', validators=[DataRequired()])
    date = DateField('Дата расчета', format='%Y-%m-%d', validators=[DataRequired()])


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html")


@app.route('/search', methods=['GET', 'POST'])
def g_curve():
    form = MyForm()
    code = None
    date = None
    ISIN = None
    if form.validate_on_submit():
        link = f"http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/{form.code.data}.json"
        params = {
            "from": form.date.data,
            "till": form.date.data
        }
        k = logic.MOEXDecoder(link, **params).data_tuple()
        return render_template('gcurve.html',
                               form=form,
                               ISIN=k[0][2],
                               name=k[0][3],
                               date=form.date.data,
                               date_json=k[0][1]
                               )
    return render_template('gcurve.html', form=form)


if __name__ == '__main__':
    app.run(port=5000, debug=True)

    # https://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/RU000A0JRVU3.json?from=2021-11-11&till=2021-11-12
