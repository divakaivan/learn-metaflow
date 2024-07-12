
from metaflow import FlowSpec, step, Parameter, conda, schedule

@schedule(hourly=True)
class ForecastFlow(FlowSpec):

    appid = Parameter('appid', default='6e5da44abe65e3320be635abfb9b0aa5')
    location = Parameter('location', default='36.1699,115.1398')

    @conda(python='3.8.10')
    @step
    def start(self):
        from openweatherdata import get_historical_weather_data, series_to_list
        lat, lon = map(float, self.location.split(','))
        self.pd_past5days = get_historical_weather_data(self.appid, lat, lon)
        self.past5days = series_to_list(self.pd_past5days)
        self.next(self.forecast)

    @conda(python='3.8.10')
    @step
    def forecast(self):
        from openweatherdata import series_to_list
        from sktime.forecasting.theta import ThetaForecaster
        import numpy
        forecaster = ThetaForecaster(sp=48)
        forecaster.fit(self.pd_past5days)
        self.pd_predictions = forecaster.predict(numpy.arange(1, 48))
        self.predictions = series_to_list(self.pd_predictions)
        self.next(self.plot)

    @conda(python='3.8.10', libraries={'seaborn': '0.11.1'})
    @step
    def plot(self):
        from sktime.utils.plotting import plot_series
        from io import BytesIO
        buf = BytesIO()
        fig, _ = plot_series(self.pd_past5days,
                             self.pd_predictions,
                             labels=['past5days', 'predictions'])
        fig.savefig(buf)
        self.plot = buf.getvalue()
        self.next(self.end)

    @conda(python='3.8.10')
    @step
    def end(self):
        pass


if __name__ == '__main__':
    ForecastFlow()