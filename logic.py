import requests
import json


class MOEXDecoder:
    def __init__(self, url, **params) -> None:
        self.url = url
        self.params = params
        try:
            r = requests.get(self.url, self.params)
            if r.status_code != 200:
                raise Exception("Код ответа: ", r.status_code)
        except requests.ConnectionError as e:
            print("OOPS!! Connection Error.\n")
            print(str(e))
        except requests.RequestException as e:
            print("OOPS!! General Error")
            print(str(e))
        self.jsn = json.loads(r.content)
        print(r.encoding)

    def columns(self) -> list:
        return self.jsn['history']['columns']

    def data_list(self) -> list:
        data = self.jsn['history']['data']
        if data:
            return data
        else:
            return None

    def data_tuple(self) -> tuple:
        data = self.jsn['history']['data']
        if data:
            output_list = []
            for i in self.jsn['history']['data']:
                output_list.append(tuple(i))
            return output_list
        else:
            return None


code = "RU000A0JRVU3"
code1 = "dfdf"
link = f"http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/{code}.json?from=2021-11-12&till=2021-11-12"
print(MOEXDecoder(link).data_tuple()[0][2])

