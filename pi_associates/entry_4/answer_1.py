from pi_associates_library.binance.binance_trade_scraper import BinanceTradeAggregator
import matplotlib.pyplot as plt
from matplotlib import animation


def compare_trade_aggregation(symbol):
    aggregator = BinanceTradeAggregator(symbol)

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax1 = fig.add_subplot(1, 1, 1)
    last_trade_id = []
    agg_price = []

    trade_id = []
    price = []

    def animate_price(i, xs, ys):
        trades = aggregator.get_trades()

        # Limit x and y lists to 100 items
        xs = [d['id'] for d in trades[-300:]]
        ys = [d['price'] for d in trades[-300:]]

        # Draw x and y lists
        ax1.clear()
        ax1.plot(xs, ys)

        # Format plot
        plt.xticks(rotation=45, ha='right')
        plt.subplots_adjust(bottom=0.30)
        plt.title('TMP102 Temperature over Time')
        plt.ylabel('Temperature (deg C)')

    # This function is called periodically from FuncAnimation
    def animate_agg(i, xs, ys):
        print(i)
        stream = aggregator.get_trade_aggregation()
        print(stream)

        # Add x and y to lists
        xs.append(stream['l'])
        ys.append(stream['p'])

        # Limit x and y lists to 100 items
        xs = xs[-100:]
        ys = ys[-100:]

        # Draw x and y lists
        ax.clear()
        ax.plot(xs, ys)

        # Format plot
        plt.xticks(rotation=45, ha='right')
        plt.subplots_adjust(bottom=0.30)
        plt.title('TMP102 Temperature over Time')
        plt.ylabel('Temperature (deg C)')

    # Set up plot to call animate() function periodically
    ani = animation.FuncAnimation(fig, animate_agg, fargs=(last_trade_id, agg_price), interval=1000)
    ani = animation.FuncAnimation(fig, animate_price, fargs=(trade_id, price), interval=1000)
    plt.show()


x1, y1 = [], []
x2, y2 = [], []


def compare():
    fig = plt.figure()
    ax = plt.axes()

    ax.plot([], [], lw=2)
    plt.xlabel('Trade ID')
    plt.ylabel('Price')

    plotlays, plotcols = [2], ["black", "red"]
    lines = []
    for index in range(2):
        lobj = ax.plot([], [], lw=2, color=plotcols[index])[0]
        lines.append(lobj)

    def init():
        for line in lines:
            line.set_data([], [])
        return lines
    #
    # x1, y1 = [], []
    # x2, y2 = [], []

    frame_num = 100
    aggregator = BinanceTradeAggregator(symbol)

    def animate(i):
        global x1, x2, y1, y2
        if i % 99 == 0:
            trades = aggregator.get_trades()

            x1 = [d['id'] for d in trades[-100:]]
            y1 = [float(d['price']) for d in trades[-100:]]
            print(x1)
            print(y1)
        # x = gps_data[0][0, i]
        # y = gps_data[1][0, i]
        # x1.append(x)
        # y1.append(y)

        # x = gps_data[0][1, i]
        # y = gps_data[1][1, i]
        # x2.append(x)
        # y2.append(y)
        stream = aggregator.get_trade_aggregation()
        x2.append(stream['l'])
        y2.append(float(stream['p']))
        x2 = x2[-100:]
        y2 = y2[-100:]
        print(x2)
        print(y2)
        print('*****')

        xlist = [x1, x2]
        ylist = [y1, y2]

        for lnum, line in enumerate(lines):
            # set data for each line separately.
            line.set_data(xlist[lnum], ylist[lnum])

        return lines

    # call the animator
    # blit=True means only re-draw the parts that have changed.
    anim = animation.FuncAnimation(fig, animate, init_func=init,
                                   frames=frame_num, interval=100, blit=True)

    plt.show()


def test():
    import matplotlib.pyplot as plt
    from matplotlib import animation
    from numpy import random

    fig = plt.figure()
    ax1 = plt.axes(xlim=(-108, -104), ylim=(31, 34))
    line, = ax1.plot([], [], lw=2)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    plotlays, plotcols = [2], ["black", "red"]
    lines = []
    for index in range(2):
        lobj = ax1.plot([], [], lw=2, color=plotcols[index])[0]
        lines.append(lobj)

    def init():
        for line in lines:
            line.set_data([], [])
        return lines

    x1, y1 = [], []
    x2, y2 = [], []

    # fake data
    frame_num = 100
    gps_data = [-104 - (4 * random.rand(2, frame_num)), 31 + (3 * random.rand(2, frame_num))]

    def animate(i):
        print(i)
        x = gps_data[0][0, i]
        y = gps_data[1][0, i]
        x1.append(x)
        y1.append(y)

        x = gps_data[0][1, i]
        y = gps_data[1][1, i]
        x2.append(x)
        y2.append(y)

        xlist = [x1, x2]
        ylist = [y1, y2]

        # for index in range(0,1):
        for lnum, line in enumerate(lines):
            line.set_data(xlist[lnum], ylist[lnum])  # set data for each line separately.

        return lines

    # call the animator.  blit=True means only re-draw the parts that have changed.
    anim = animation.FuncAnimation(fig, animate, init_func=init,
                                   frames=frame_num, interval=10, blit=True)

    plt.show()

if __name__ == '__main__':
    # aggregator = BinanceTradeAggregator('btcusdt')
    # asyncio.get_event_loop().run_until_complete(aggregator.live_aggregate('btcusdt'))
    symbol = 'btcusdt'
    # asyncio.get_event_loop().run_until_complete(compare_trade_agg(symbol))
    # compare_trade_aggregation(symbol)
    compare()
    # aggregator = BinanceTradeAggregator('btcusdt')
    # trades = aggregator.get_trades()
    # print(len(trades))
    # print(trades[0])
    # print(trades[-1])
