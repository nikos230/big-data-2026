import matplotlib.pyplot as plt


configs_A = ['1 Core\n2GB RAM', '2 Cores\n4GB RAM', '4 Cores\n8GB RAM']
times_A = [59.85, 41.33, 20.57]

fig1, ax1 = plt.subplots(figsize=(8, 6))
bars1 = ax1.bar(configs_A, times_A, color=['#E63946', '#457B9D', '#2A9D8F'], width=0.5, edgecolor='black')

ax1.set_xlabel('Resources', fontsize=12, fontweight='bold')
ax1.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
ax1.set_title('Execution Time vs Resources', fontsize=14, fontweight='bold')

for bar in bars1:
    yval = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2.0, yval + 0.5, f'{yval} s', 
            ha='center', va='bottom', fontsize=11, fontweight='bold')

ax1.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig('/home/nikos/bigdata-dsml/report/images/q4_A_times.png')


configs_B = ['2 Execs\n4 Cores\n8GB RAM', '4 Execs\n2 Cores\n4GB RAM', '8 Execs\n1 Core\n2GB RAM']
times_B = [22.03, 23.22, 26.00]

fig2, ax2 = plt.subplots(figsize=(8, 6))
bars2 = ax2.bar(configs_B, times_B, color=['#F4A261', '#E76F51', '#264653'], width=0.5, edgecolor='black')

ax2.set_xlabel('Cluster Configuration', fontsize=12, fontweight='bold')
ax2.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
ax2.set_title('Execution Time vs Cluster Configuration', fontsize=14, fontweight='bold')

for bar in bars2:
    yval = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2.0, yval + 0.2, f'{yval} s', 
            ha='center', va='bottom', fontsize=11, fontweight='bold')

ax2.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()

plt.savefig('/home/nikos/bigdata-dsml/report/images/q4_B_times.png')