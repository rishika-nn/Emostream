import asyncio
import aiohttp
from rich.console import Console
from rich.progress import Progress
import time
import random
from datetime import datetime

console = Console()

async def send_emoji(session, data):
    """Send a single emoji request"""
    url = 'http://localhost:5000/send_emoji'
    try:
        async with session.post(url, json=data) as response:
            return await response.json()
    except Exception as e:
        console.print(f"[red]Error sending emoji: {e}[/red]")
        return None

async def run_test_scenario(scenario_name, emoji_counts):
    """Run a specific test scenario with precise timing"""
    console.print(f"\n[bold blue]Running {scenario_name}[/bold blue]")
    
    async with aiohttp.ClientSession() as session:
        with Progress() as progress:
            task = progress.add_task(f"[cyan]Sending emojis...", total=sum(count for _, count in emoji_counts))
            
            # Track start time for the scenario
            scenario_start_time = time.time()
            current_window_start = scenario_start_time
            
            for emoji_type, count in emoji_counts:
                tasks = []
                emojis_sent_in_window = 0
                
                for i in range(count):
                    current_time = time.time()
                    
                    # If we've moved to a new 2-second window, log the previous window's count
                    if current_time - current_window_start >= 2.0:
                        console.print(f"Window {datetime.fromtimestamp(current_window_start).strftime('%H:%M:%S')}"
                                    f" - {datetime.fromtimestamp(current_window_start + 2).strftime('%H:%M:%S')}: "
                                    f"Sent {emojis_sent_in_window} {emoji_type}")
                        current_window_start = current_time
                        emojis_sent_in_window = 0
                    
                    data = {
                        'user_id': f'test_user_{i}',
                        'emoji_type': emoji_type,
                        'timestamp': int(current_time * 1000)  # Millisecond precision
                    }
                    tasks.append(send_emoji(session, data))
                    emojis_sent_in_window += 1
                    
                    # Send in batches of 100 or when window boundary approaches
                    if len(tasks) >= 100 or (current_time - current_window_start) >= 1.9:
                        await asyncio.gather(*tasks)
                        progress.update(task, advance=len(tasks))
                        tasks = []
                        
                        # Small delay to control rate
                        await asyncio.sleep(0.01)
                
                # Send any remaining tasks
                if tasks:
                    await asyncio.gather(*tasks)
                    progress.update(task, advance=len(tasks))
                
                # Log final window count
                console.print(f"Final Window for {emoji_type}: {emojis_sent_in_window} emojis")
                
                # Wait for current window to complete before starting next emoji type
                await asyncio.sleep(2.0 - ((time.time() - current_window_start) % 2.0))

async def main():
    test_scenarios = [
        ("Scenario 1: Single Emoji Burst - Testing 2-second Window", [
            ('👍', 1200)  # Should see ~600 emojis per 2-second window
        ]),
        
        ("Scenario 2: Multiple Emoji Types - Window Boundary Test", [
            ('❤️', 800),   # ~400 per window
            ('😊', 1100),  # ~550 per window
            ('🎉', 950)    # ~475 per window
        ]),
        
        ("Scenario 3: High-Volume Mixed Test", [
            ('👍', 500),   # ~250 per window
            ('❤️', 1200),  # ~600 per window
            ('😊', 300),   # ~150 per window
            ('🎉', 1500),  # ~750 per window
            ('👏', 700)    # ~350 per window
        ])
    ]
    
    for scenario_name, emoji_counts in test_scenarios:
        console.print(f"\n[bold yellow]Starting {scenario_name}[/bold yellow]")
        await run_test_scenario(scenario_name, emoji_counts)
        console.print(f"[green]Completed {scenario_name}[/green]")
        
        # Wait for processing to complete before next scenario
        await asyncio.sleep(4)  # Wait for two full windows

if __name__ == "__main__":
    console.print("[bold green]Starting EmoStream Test Suite[/bold green]")
    console.print("\n[yellow]Please ensure:[/yellow]")
    console.print("1. Flask app is running (python flaskapp.py)")
    console.print("2. Spark streaming job is running (python sparktask.py)")
    console.print("3. Kafka and Zookeeper are running")
    
    proceed = console.input("\nProceed with tests? (y/n): ")
    if proceed.lower() == 'y':
        asyncio.run(main())
    else:
        console.print("[red]Test cancelled[/red]")