import matplotlib.pyplot as plt

labels_all = ['DF (χωρίς UDF)', 'DF (με UDF)', 'RDD']
times_all = [0.2224, 0.2233, 10.6075]

labels_df = ['DF (χωρίς UDF)', 'DF (με UDF)']
times_df = [0.2224, 0.2233]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))

bars1 = ax1.bar(labels_all, times_all, color='#7b9ced', width=0.4)
ax1.set_ylabel('Χρόνος Εκτέλεσης (seconds)')
ax1.set_title('Συνολική Σύγκριση')
ax1.set_ylim(0, 12)
ax1.bar_label(bars1, fmt='%.3f', padding=3)

# bars2 = ax2.bar(labels_df, times_df, color='#1e711e', width=0.4)
# ax2.set_ylabel('Χρόνος Εκτέλεσης (seconds)')
# ax2.set_title('Σύγκριση DataFrame (Zoom)')
# ax2.set_ylim(0.220, 0.225) # Zoomed in Y-axis
# ax2.bar_label(bars2, fmt='%.4f', padding=3)

plt.suptitle('Αποτελέσματα Query 1 (2 executors: 1 core, 2GB memory)', fontsize=14)
plt.tight_layout()
plt.show()