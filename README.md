# Peer-to-Peer network generator
Implemented a gossip protocol over a peer-to-peer network such that the degree distribution of the peer nodes follows power law.<br />
<img width="1000" height="600" alt="powerLawP2P" src="https://github.com/user-attachments/assets/a9d3850e-0f16-4d2b-928b-d95657fcd651" />

## Steps to Run the Code

1. **Set up the config file**: Create a `config.csv` file with the IP addresses and port numbers of the seed nodes.
2. **Set up the peers file**: Create a `peers.csv` file with the IP addresses and port numbers of the peer nodes.
3. **Run the codes**: Execute the `seed.py` & `peer.py` script on  on seperate terminals.

   ```bash
   python seed.py
   ```
   ```bash
   python peer.py
   ```

4. Once there does not seem to be any change in terminal outputs, run networPlot.py to check degree distribution
   ```bash
   python networPlot.py
   ```
   
**Switch the main function in seed.py and peer.py to run on seperate machines** (dont forget to update the config file):
<img width="343" height="286" alt="update config file" src="https://github.com/user-attachments/assets/0e0846b8-9ecd-4830-849d-53593b122e9b" />

