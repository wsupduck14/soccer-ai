from karpathy_class import *
import pandas as pd


# wget https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt
with open("input.txt", "r", encoding="utf-8") as f:
    text = f.read()

# here are all the unique characters that occur in this text
chars = sorted(list(set(text)))
num_tokens = len(chars)

# create a mapping from characters to integers
stoi = {ch: i for i, ch in enumerate(chars)}
itos = {i: ch for i, ch in enumerate(chars)}
# encoder: take a string, output a list of integers
def encode(s): return [stoi[c] for c in s]
# decoder: take a list of integers, output a string
def decode(l): return "".join([itos[i] for i in l])

# Train and test splits


model, optimizer = init_model(encode(text), num_tokens)

loop_time_series = pd.Series(dtype=float)

for i in range(max_iters):
    # every once in a while evaluate the loss on train and val sets
    if i % eval_interval == 0 or i == 1 or i == max_iters - 1:
        avg_train_pass = loop_time_series.loc[i - eval_interval: i].mean()
        losses, loss_calc_time = estimate_loss()
        print(f"step {i}: train loss {losses['train']:.4f}% val loss {losses['val']:.4f} | avg loop time {avg_train_pass:.4f}s loss calc time {loss_calc_time:.4f}s")

    start_loop = perf_counter()
    # sample a batch of data
    xb, yb = get_batch("train")

    # evaluate the loss
    logits, loss = model(xb, yb)
    optimizer.zero_grad(set_to_none=True)
    loss.backward()
    optimizer.step()
    end_loop = perf_counter()
    # print(f"loop {iter} time {end_loop - start_loop}")
    loop_time = end_loop - start_loop
    loop_time_series = pd.concat([loop_time_series, pd.Series([loop_time], dtype=float)], ignore_index=True)

# generate from the model
context = torch.zeros((1, 1), dtype=torch.long, device=device)
print(decode(model.generate(context, max_new_tokens=500)[0].tolist()))
# open('more.txt', 'w').write(decode(m.generate(context, max_new_tokens=10000)[0].tolist()))
