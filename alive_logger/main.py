from eventus_sdk import minion


@minion
def run(ctx):
    ctx.log.info("я живой")
